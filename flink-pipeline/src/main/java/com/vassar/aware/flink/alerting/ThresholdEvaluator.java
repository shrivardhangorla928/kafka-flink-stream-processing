package com.vassar.aware.flink.alerting;

import com.vassar.aware.common.Alert;
import com.vassar.aware.common.AlertIds;
import com.vassar.aware.common.MeasurementUnit;
import com.vassar.aware.common.SensorType;
import com.vassar.aware.common.Severity;
import com.vassar.aware.common.StationWindowAggregate;
import com.vassar.aware.flink.config.ThresholdRule;
import com.vassar.aware.flink.config.ThresholdRuleSet;

import java.io.Serializable;
import java.time.Clock;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * Decides whether a finished window is worth waking somebody up for.
 *
 * <p>Contains no Flink type at all. Threshold policy is the part of this job that a hydrologist
 * will want to reason about and that a reviewer will want to see tested at every boundary, and
 * that is only comfortable if the rules can be exercised without a cluster. The operator wrapping
 * it is deliberately thin.</p>
 *
 * <p>The alert id comes from {@link AlertIds#deterministicId} rather than a random UUID: the Kafka
 * sinks run at-least-once and a restore from checkpoint re-emits already-emitted windows, so the
 * same window must always produce the same id for the alert service's upsert to collapse the
 * duplicate instead of raising the same flood warning twice.</p>
 */
public final class ThresholdEvaluator implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final DateTimeFormatter WINDOW_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'").withZone(java.time.ZoneOffset.UTC);

    private final ThresholdRuleSet rules;
    private final Clock clock;

    public ThresholdEvaluator(ThresholdRuleSet rules) {
        this(rules, Clock.systemUTC());
    }

    /** Clock injected so tests can assert on {@code generatedAt} without racing wall-clock time. */
    public ThresholdEvaluator(ThresholdRuleSet rules, Clock clock) {
        this.rules = Objects.requireNonNull(rules, "rules");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * At most one alert per window: the highest breached band, or empty when the window is calm,
     * when the sensor type has no configured policy, or when the aggregate is unusable.
     */
    public Optional<Alert> evaluate(StationWindowAggregate aggregate) {
        if (aggregate == null || aggregate.getSensorType() == null || aggregate.getStationId() == null) {
            return Optional.empty();
        }
        ThresholdRule rule = rules.ruleFor(aggregate.getSensorType());
        if (rule == null) {
            return Optional.empty();
        }
        Severity severity = rule.highestBreachedBand(aggregate.getAggregatedValue());
        if (severity == null) {
            return Optional.empty();
        }
        return Optional.of(toAlert(aggregate, rule, severity));
    }

    private Alert toAlert(StationWindowAggregate aggregate, ThresholdRule rule, Severity severity) {
        double threshold = rule.thresholdFor(severity);
        MeasurementUnit unit = resolveUnit(aggregate);

        Alert alert = new Alert();
        alert.setAlertId(AlertIds.deterministicId(
                aggregate.getStationId(),
                aggregate.getSensorType(),
                aggregate.getWindowStart(),
                aggregate.getWindowEnd(),
                severity));
        alert.setStationId(aggregate.getStationId());
        alert.setStationName(aggregate.getStationName());
        alert.setDistrictId(aggregate.getDistrictId());
        alert.setSensorType(aggregate.getSensorType());
        alert.setSeverity(severity);
        alert.setMetric(rule.getMetric());
        alert.setObservedValue(aggregate.getAggregatedValue());
        alert.setThresholdValue(threshold);
        alert.setUnit(unit);
        alert.setWindowStart(aggregate.getWindowStart());
        alert.setWindowEnd(aggregate.getWindowEnd());
        alert.setLatitude(aggregate.getLatitude());
        alert.setLongitude(aggregate.getLongitude());
        alert.setGeneratedAt(Instant.now(clock));
        alert.setMessage(describe(aggregate, severity, threshold, unit));
        return alert;
    }

    private static MeasurementUnit resolveUnit(StationWindowAggregate aggregate) {
        if (aggregate.getUnit() != null) {
            return aggregate.getUnit();
        }
        SensorType sensorType = aggregate.getSensorType();
        return sensorType == null ? null : sensorType.getUnit();
    }

    /**
     * A sentence an operations officer can act on without opening a dashboard: which station,
     * what was measured, what the limit was, and over which window.
     */
    private static String describe(StationWindowAggregate aggregate,
                                   Severity severity,
                                   double threshold,
                                   MeasurementUnit unit) {
        String symbol = unit == null ? "" : unit.getSymbol();
        String station = aggregate.getStationName() == null || aggregate.getStationName().isBlank()
                ? aggregate.getStationId()
                : aggregate.getStationName() + " (" + aggregate.getStationId() + ")";
        return String.format(Locale.ROOT,
                "%s severity: %s recorded %s of %.2f %s between %s and %s, at or above the %s threshold of %.2f %s.",
                severity,
                station,
                readableSensorType(aggregate.getSensorType()),
                aggregate.getAggregatedValue(),
                symbol,
                format(aggregate.getWindowStart()),
                format(aggregate.getWindowEnd()),
                severity,
                threshold,
                symbol);
    }

    private static String readableSensorType(SensorType sensorType) {
        return sensorType.name().toLowerCase(Locale.ROOT).replace('_', ' ');
    }

    private static String format(Instant instant) {
        return instant == null ? "an unknown time" : WINDOW_FORMAT.format(instant);
    }
}
