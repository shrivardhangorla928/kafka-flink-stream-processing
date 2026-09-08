package com.stream.processing.common;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The enums are not plain labels: the Flink job selects its aggregation function from
 * {@link SensorType#getAggregationKind()} and picks the highest breached band using
 * {@link Severity#rank()}. Both are behaviour worth pinning.
 */
class EnumContractTest {

    @Test
    @DisplayName("each sensor type carries the aggregation and unit the pipeline applies to it")
    void sensorTypesCarryTheirAggregationAndUnit() {
        assertThat(SensorType.RAINFALL.getAggregationKind()).isEqualTo(SensorType.AggregationKind.SUM);
        assertThat(SensorType.RAINFALL.getUnit()).isEqualTo(MeasurementUnit.MM);

        assertThat(SensorType.RESERVOIR_LEVEL.getAggregationKind()).isEqualTo(SensorType.AggregationKind.MAX);
        assertThat(SensorType.RESERVOIR_LEVEL.getUnit()).isEqualTo(MeasurementUnit.PERCENT);

        assertThat(SensorType.RIVER_LEVEL.getAggregationKind()).isEqualTo(SensorType.AggregationKind.MAX);
        assertThat(SensorType.RIVER_LEVEL.getUnit()).isEqualTo(MeasurementUnit.METRE);
    }

    @ParameterizedTest
    @EnumSource(SensorType.class)
    @DisplayName("no sensor type is missing its aggregation kind or unit")
    void everySensorTypeIsFullyDescribed(SensorType type) {
        assertThat(type.getAggregationKind()).isNotNull();
        assertThat(type.getUnit()).isNotNull();
    }

    @Test
    @DisplayName("severity ranks increase from INFO to EXTREME")
    void severityIsOrdered() {
        assertThat(Severity.INFO.rank()).isLessThan(Severity.WARNING.rank());
        assertThat(Severity.WARNING.rank()).isLessThan(Severity.SEVERE.rank());
        assertThat(Severity.SEVERE.rank()).isLessThan(Severity.EXTREME.rank());
    }

    @Test
    @DisplayName("isAtLeast implements the minimum-severity filter the alert query API exposes")
    void isAtLeastIsInclusive() {
        assertThat(Severity.SEVERE.isAtLeast(Severity.WARNING)).isTrue();
        assertThat(Severity.SEVERE.isAtLeast(Severity.SEVERE)).isTrue();
        assertThat(Severity.WARNING.isAtLeast(Severity.SEVERE)).isFalse();
    }

    @Test
    @DisplayName("isAtLeast treats a null bound as no match rather than throwing")
    void isAtLeastHandlesNull() {
        assertThat(Severity.EXTREME.isAtLeast(null)).isFalse();
    }

    @Test
    @DisplayName("units expose the symbol used in alert messages")
    void unitsCarryTheirSymbol() {
        assertThat(MeasurementUnit.MM.getSymbol()).isEqualTo("mm");
        assertThat(MeasurementUnit.METRE.getSymbol()).isEqualTo("m");
        assertThat(MeasurementUnit.PERCENT.getSymbol()).isEqualTo("%");
    }

    @Test
    @DisplayName("topic names are versioned so a breaking schema change can run alongside the old one")
    void topicNamesAreVersioned() {
        assertThat(Topics.RAW_TELEMETRY).isEqualTo("telemetry.raw.v1");
        assertThat(Topics.AGGREGATED_TELEMETRY).isEqualTo("telemetry.aggregated.v1");
        assertThat(Topics.GENERATED_ALERTS).isEqualTo("alerts.generated.v1");
        assertThat(Topics.DEAD_LETTER).isEqualTo("telemetry.dlq.v1");
    }
}
