package com.stream.processing.flink.rainfall;

import com.stream.processing.common.Alert;
import com.stream.processing.common.AlertIds;
import com.stream.processing.common.MeasurementUnit;
import com.stream.processing.common.SensorReading;
import com.stream.processing.common.SensorType;
import com.stream.processing.common.Severity;

import java.io.Serializable;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * Decides whether a station's rolling rainfall accumulation is worth waking somebody up for.
 *
 * <p>Handed the raw readings a station produced over roughly the last 24 hours, sums them into
 * each of the six rolling windows in {@link RainfallAlertPolicy#WINDOW_HOURS_ASCENDING} (1h, 2h,
 * 3h, 6h, 12h, 24h) and keeps the worst — a flash flood shows up in the 1h window long before the
 * 24h total moves, while a slow saturating monsoon only shows up in the 24h total. Buffering and
 * eviction of the readings is the Flink-facing operator's job; this class has no Flink type and no
 * state, so it can be tested without a cluster.</p>
 *
 * <p>The alert id comes from {@link AlertIds#deterministicId} rather than a random UUID: Kafka
 * sinks run at-least-once and a checkpoint restore can re-emit an already-evaluated window, so the
 * id has to be stable for the alert service's upsert to collapse the duplicate.</p>
 */
public final class RainfallAccumulationEvaluator implements Serializable {

    private static final long serialVersionUID = 1L;

    public RainfallAccumulationEvaluator() {
        this(Clock.systemUTC());
    }

    /** Clock injected so tests can assert on {@code generatedAt} without racing wall-clock time. */
    public RainfallAccumulationEvaluator(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    private final Clock clock;

    /**
     * @param stationId required
     * @param stationName may be null/blank
     * @param districtId may be null
     * @param latitude station latitude
     * @param longitude station longitude
     * @param readingsLast24h every RAINFALL reading Flink still has buffered for this station,
     *                        roughly covering the last 24h; readings outside whichever window is
     *                        currently being summed are safely ignored, so extra history is tolerated
     * @param latestEventTime the event time of the reading that triggered this evaluation
     */
    public Optional<Alert> evaluate(String stationId,
                                     String stationName,
                                     String districtId,
                                     double latitude,
                                     double longitude,
                                     List<SensorReading> readingsLast24h,
                                     Instant latestEventTime) {
        if (stationId == null || stationId.isBlank()
                || latestEventTime == null
                || readingsLast24h == null || readingsLast24h.isEmpty()) {
            return Optional.empty();
        }

        int bestWindowHours = -1;
        double bestSum = 0.0d;
        Severity bestSeverity = null;

        for (int windowHours : RainfallAlertPolicy.WINDOW_HOURS_ASCENDING) {
            double sum = sumWithinWindow(readingsLast24h, latestEventTime, windowHours);
            Optional<Severity> severity = RainfallAlertPolicy.severityFor(windowHours, sum);
            if (severity.isEmpty()) {
                continue;
            }
            // Ties favour the LARGER window: iterating windows ascending and updating on ">=" means
            // that when two windows reach the same severity, the later (larger) one overwrites the
            // earlier (smaller) one. A short burst and a sustained soak that both just clear
            // "severe" are not equally informative -- the sustained one is the stronger, harder to
            // fake evidence of a real flood risk, so it is the one reported to the operator.
            if (bestSeverity == null || severity.get().rank() >= bestSeverity.rank()) {
                bestSeverity = severity.get();
                bestWindowHours = windowHours;
                bestSum = sum;
            }
        }

        if (bestSeverity == null) {
            return Optional.empty();
        }

        return Optional.of(toAlert(stationId, stationName, districtId, latitude, longitude,
                latestEventTime, bestWindowHours, bestSum, bestSeverity));
    }

    /**
     * Sum of every reading whose event time falls within
     * {@code (latestEventTime - windowHours, latestEventTime]}, i.e. the last N hours inclusive
     * of the triggering reading itself.
     */
    private static double sumWithinWindow(List<SensorReading> readings,
                                          Instant latestEventTime,
                                          int windowHours) {
        Instant windowStart = latestEventTime.minus(windowHours, ChronoUnit.HOURS);
        double sum = 0.0d;
        for (SensorReading reading : readings) {
            if (reading == null || reading.getEventTime() == null) {
                continue;
            }
            Instant eventTime = reading.getEventTime();
            if (eventTime.isAfter(windowStart) && !eventTime.isAfter(latestEventTime)) {
                sum += reading.getValue();
            }
        }
        return sum;
    }

    private Alert toAlert(String stationId,
                          String stationName,
                          String districtId,
                          double latitude,
                          double longitude,
                          Instant latestEventTime,
                          int triggeringWindowHours,
                          double accumulatedMm,
                          Severity severity) {
        Instant windowStart = latestEventTime.minus(triggeringWindowHours, ChronoUnit.HOURS);
        Instant windowEnd = latestEventTime;
        String bandLabel = RainfallAlertPolicy.bandLabelFor(triggeringWindowHours, accumulatedMm);
        double thresholdValue = RainfallAlertPolicy.bandLowerBoundFor(triggeringWindowHours, bandLabel);

        Alert alert = new Alert();
        alert.setAlertId(AlertIds.deterministicId(
                stationId, SensorType.RAINFALL, windowStart, windowEnd, severity));
        alert.setStationId(stationId);
        alert.setStationName(stationName);
        alert.setDistrictId(districtId);
        alert.setSensorType(SensorType.RAINFALL);
        alert.setSeverity(severity);
        alert.setMetric("RAINFALL_ACCUMULATION_" + triggeringWindowHours + "H");
        alert.setObservedValue(accumulatedMm);
        alert.setThresholdValue(thresholdValue);
        alert.setUnit(MeasurementUnit.MM);
        alert.setWindowStart(windowStart);
        alert.setWindowEnd(windowEnd);
        alert.setLatitude(latitude);
        alert.setLongitude(longitude);
        alert.setGeneratedAt(Instant.now(clock));
        alert.setMessage(describe(stationId, stationName, severity, triggeringWindowHours,
                accumulatedMm, bandLabel, thresholdValue));
        return alert;
    }

    /**
     * A sentence an operations officer can act on without opening a dashboard: which station, how
     * much rain, over what window, and which readable category it crossed into.
     */
    private static String describe(String stationId,
                                   String stationName,
                                   Severity severity,
                                   int windowHours,
                                   double accumulatedMm,
                                   String bandLabel,
                                   double thresholdValue) {
        String station = stationName == null || stationName.isBlank()
                ? stationId
                : stationName + " (" + stationId + ")";
        return String.format(Locale.ROOT,
                "%s severity: %s recorded %.2f mm of rainfall accumulated over the last %d hours, "
                        + "classified as %s (at or above %.2f mm).",
                severity,
                station,
                accumulatedMm,
                windowHours,
                RainfallAlertPolicy.humanize(bandLabel),
                thresholdValue);
    }
}
