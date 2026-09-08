package com.vassar.aware.flink.alerting;

import com.vassar.aware.common.Alert;
import com.vassar.aware.common.AlertIds;
import com.vassar.aware.common.MeasurementUnit;
import com.vassar.aware.common.SensorType;
import com.vassar.aware.common.Severity;
import com.vassar.aware.common.StationWindowAggregate;
import com.vassar.aware.flink.config.PipelineConfig;
import com.vassar.aware.flink.config.ThresholdRule;
import com.vassar.aware.flink.config.ThresholdRuleSet;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Boundary-by-boundary verification of the alerting policy. */
class ThresholdEvaluatorTest {

    private static final Instant WINDOW_START = Instant.parse("2026-08-11T05:00:00Z");
    private static final Instant WINDOW_END = Instant.parse("2026-08-11T05:05:00Z");
    private static final Instant NOW = Instant.parse("2026-08-11T05:05:01Z");

    private final ThresholdEvaluator evaluator = new ThresholdEvaluator(
            defaultRules(), Clock.fixed(NOW, ZoneOffset.UTC));

    private static ThresholdRuleSet defaultRules() {
        return PipelineConfig.from(ParameterTool.fromMap(Map.of()), name -> null).getThresholds();
    }

    private static StationWindowAggregate aggregate(SensorType sensorType, double aggregatedValue) {
        StationWindowAggregate aggregate = new StationWindowAggregate();
        aggregate.setStationId("STN-42");
        aggregate.setStationName("Kadapa");
        aggregate.setDistrictId("D-KDP");
        aggregate.setSensorType(sensorType);
        aggregate.setUnit(sensorType.getUnit());
        aggregate.setWindowStart(WINDOW_START);
        aggregate.setWindowEnd(WINDOW_END);
        aggregate.setAggregatedValue(aggregatedValue);
        aggregate.setLatitude(14.46);
        aggregate.setLongitude(78.82);
        return aggregate;
    }

    @ParameterizedTest(name = "RAINFALL {0} mm -> {1}")
    @CsvSource({
            "0.0,     NONE",
            "14.999,  NONE",
            "15.0,    WARNING",
            "29.999,  WARNING",
            "30.0,    SEVERE",
            "49.999,  SEVERE",
            "50.0,    EXTREME",
            "180.0,   EXTREME"
    })
    void appliesRainfallBandsInclusively(double value, String expected) {
        assertSeverity(SensorType.RAINFALL, value, expected);
    }

    @ParameterizedTest(name = "RESERVOIR_LEVEL {0}% -> {1}")
    @CsvSource({
            "0.0,     NONE",
            "84.999,  NONE",
            "85.0,    WARNING",
            "94.999,  WARNING",
            "95.0,    SEVERE",
            "99.999,  SEVERE",
            "100.0,   EXTREME",
            "104.0,   EXTREME"
    })
    void appliesReservoirBandsInclusively(double value, String expected) {
        assertSeverity(SensorType.RESERVOIR_LEVEL, value, expected);
    }

    @ParameterizedTest(name = "RIVER_LEVEL {0} m -> {1}")
    @CsvSource({
            "0.0,    NONE",
            "7.999,  NONE",
            "8.0,    WARNING",
            "9.999,  WARNING",
            "10.0,   SEVERE",
            "11.999, SEVERE",
            "12.0,   EXTREME",
            "18.5,   EXTREME"
    })
    void appliesRiverBandsInclusively(double value, String expected) {
        assertSeverity(SensorType.RIVER_LEVEL, value, expected);
    }

    private void assertSeverity(SensorType sensorType, double value, String expected) {
        Optional<Alert> alert = evaluator.evaluate(aggregate(sensorType, value));
        if ("NONE".equals(expected)) {
            assertThat(alert).isEmpty();
        } else {
            assertThat(alert).isPresent();
            assertThat(alert.get().getSeverity()).isEqualTo(Severity.valueOf(expected));
        }
    }

    @Test
    void emitsNothingForACalmWindow() {
        assertThat(evaluator.evaluate(aggregate(SensorType.RAINFALL, 1.5d))).isEmpty();
    }

    @Test
    void emitsOnlyTheHighestBreachedBand() {
        // 60 mm clears all three rainfall bands; a single EXTREME alert must come out, not three.
        Optional<Alert> alert = evaluator.evaluate(aggregate(SensorType.RAINFALL, 60.0d));

        assertThat(alert).isPresent();
        assertThat(alert.get().getSeverity()).isEqualTo(Severity.EXTREME);
        assertThat(alert.get().getThresholdValue()).isEqualTo(PipelineConfig.DEFAULT_RAINFALL_EXTREME);
    }

    @Test
    void populatesEveryFieldTheAlertServiceConsumes() {
        Alert alert = evaluator.evaluate(aggregate(SensorType.RAINFALL, 32.5d)).orElseThrow();

        assertThat(alert.getStationId()).isEqualTo("STN-42");
        assertThat(alert.getStationName()).isEqualTo("Kadapa");
        assertThat(alert.getDistrictId()).isEqualTo("D-KDP");
        assertThat(alert.getSensorType()).isEqualTo(SensorType.RAINFALL);
        assertThat(alert.getSeverity()).isEqualTo(Severity.SEVERE);
        assertThat(alert.getMetric()).isEqualTo(PipelineConfig.METRIC_RAINFALL);
        assertThat(alert.getObservedValue()).isEqualTo(32.5d);
        assertThat(alert.getThresholdValue()).isEqualTo(PipelineConfig.DEFAULT_RAINFALL_SEVERE);
        assertThat(alert.getUnit()).isEqualTo(MeasurementUnit.MM);
        assertThat(alert.getWindowStart()).isEqualTo(WINDOW_START);
        assertThat(alert.getWindowEnd()).isEqualTo(WINDOW_END);
        assertThat(alert.getLatitude()).isEqualTo(14.46d);
        assertThat(alert.getLongitude()).isEqualTo(78.82d);
        assertThat(alert.getGeneratedAt()).isEqualTo(NOW);
    }

    @Test
    void usesTheMetricNameOfTheSensorType() {
        assertThat(evaluator.evaluate(aggregate(SensorType.RESERVOIR_LEVEL, 96.0d)).orElseThrow().getMetric())
                .isEqualTo(PipelineConfig.METRIC_RESERVOIR);
        assertThat(evaluator.evaluate(aggregate(SensorType.RIVER_LEVEL, 11.0d)).orElseThrow().getMetric())
                .isEqualTo(PipelineConfig.METRIC_RIVER);
    }

    @Test
    void derivesAStableAlertIdFromTheFactsThatProducedIt() {
        Alert first = evaluator.evaluate(aggregate(SensorType.RAINFALL, 32.5d)).orElseThrow();
        // The same window replayed after a checkpoint restore, with a different reading count.
        StationWindowAggregate replay = aggregate(SensorType.RAINFALL, 32.5d);
        replay.setReadingCount(99L);
        Alert second = evaluator.evaluate(replay).orElseThrow();

        assertThat(second.getAlertId()).isEqualTo(first.getAlertId());
        assertThat(first.getAlertId()).isEqualTo(AlertIds.deterministicId(
                "STN-42", SensorType.RAINFALL, WINDOW_START, WINDOW_END, Severity.SEVERE));
    }

    @Test
    void givesADifferentIdToADifferentWindowOrSeverity() {
        Alert severe = evaluator.evaluate(aggregate(SensorType.RAINFALL, 32.5d)).orElseThrow();
        Alert extreme = evaluator.evaluate(aggregate(SensorType.RAINFALL, 55.0d)).orElseThrow();

        StationWindowAggregate laterWindow = aggregate(SensorType.RAINFALL, 32.5d);
        laterWindow.setWindowStart(WINDOW_END);
        laterWindow.setWindowEnd(WINDOW_END.plusSeconds(300));
        Alert next = evaluator.evaluate(laterWindow).orElseThrow();

        assertThat(severe.getAlertId()).isNotEqualTo(extreme.getAlertId());
        assertThat(severe.getAlertId()).isNotEqualTo(next.getAlertId());
    }

    @Test
    void writesAMessageAnOperatorCanActOn() {
        Alert alert = evaluator.evaluate(aggregate(SensorType.RIVER_LEVEL, 10.5d)).orElseThrow();

        assertThat(alert.getMessage())
                .startsWith("SEVERE severity:")
                .contains("Kadapa (STN-42)")
                .contains("river level")
                .contains("10.50 m")
                .contains("threshold of 10.00 m")
                .contains("2026-08-11 05:00:00 UTC")
                .contains("2026-08-11 05:05:00 UTC");
    }

    @Test
    void fallsBackToTheStationIdWhenTheNameIsMissing() {
        StationWindowAggregate anonymous = aggregate(SensorType.RAINFALL, 20.0d);
        anonymous.setStationName(null);

        assertThat(evaluator.evaluate(anonymous).orElseThrow().getMessage()).contains("STN-42 recorded");
    }

    @Test
    void takesTheUnitFromTheSensorTypeWhenTheAggregateOmitsIt() {
        StationWindowAggregate unitless = aggregate(SensorType.RESERVOIR_LEVEL, 99.0d);
        unitless.setUnit(null);

        Alert alert = evaluator.evaluate(unitless).orElseThrow();

        assertThat(alert.getUnit()).isEqualTo(MeasurementUnit.PERCENT);
        assertThat(alert.getMessage()).contains("99.00 %");
    }

    @Test
    void honoursOverriddenThresholds() {
        Map<SensorType, ThresholdRule> tightened = new EnumMap<>(SensorType.class);
        tightened.put(SensorType.RAINFALL, new ThresholdRule(PipelineConfig.METRIC_RAINFALL, 1.0d, 2.0d, 3.0d));
        ThresholdEvaluator strict = new ThresholdEvaluator(new ThresholdRuleSet(tightened));

        Alert alert = strict.evaluate(aggregate(SensorType.RAINFALL, 2.5d)).orElseThrow();

        assertThat(alert.getSeverity()).isEqualTo(Severity.SEVERE);
        assertThat(alert.getThresholdValue()).isEqualTo(2.0d);
    }

    @Test
    void staysQuietWhenTheSensorTypeHasNoConfiguredPolicy() {
        ThresholdEvaluator rainfallOnly = new ThresholdEvaluator(new ThresholdRuleSet(Map.of(
                SensorType.RAINFALL, new ThresholdRule(PipelineConfig.METRIC_RAINFALL, 15.0d, 30.0d, 50.0d))));

        assertThat(rainfallOnly.evaluate(aggregate(SensorType.RIVER_LEVEL, 500.0d))).isEmpty();
    }

    @Test
    void ignoresAggregatesItCannotIdentify() {
        StationWindowAggregate noStation = aggregate(SensorType.RAINFALL, 99.0d);
        noStation.setStationId(null);
        StationWindowAggregate noType = aggregate(SensorType.RAINFALL, 99.0d);
        noType.setSensorType(null);

        assertThat(evaluator.evaluate(null)).isEmpty();
        assertThat(evaluator.evaluate(noStation)).isEmpty();
        assertThat(evaluator.evaluate(noType)).isEmpty();
    }

    @Test
    void treatsNaNAsUnbreached() {
        assertThat(evaluator.evaluate(aggregate(SensorType.RAINFALL, Double.NaN))).isEmpty();
    }
}
