package com.stream.processing.common;

/**
 * The kind of quantity a station reports. Determines which aggregation function and
 * which threshold set the pipeline applies downstream.
 */
public enum SensorType {

    /** Rainfall accumulated since the previous reading, in millimetres. Aggregated by sum. */
    RAINFALL(AggregationKind.SUM, MeasurementUnit.MM),

    /** Reservoir storage expressed as a percentage of full reservoir level. Aggregated by max. */
    RESERVOIR_LEVEL(AggregationKind.MAX, MeasurementUnit.PERCENT),

    /** River gauge height above datum, in metres. Aggregated by max. */
    RIVER_LEVEL(AggregationKind.MAX, MeasurementUnit.METRE);

    private final AggregationKind aggregationKind;
    private final MeasurementUnit unit;

    SensorType(AggregationKind aggregationKind, MeasurementUnit unit) {
        this.aggregationKind = aggregationKind;
        this.unit = unit;
    }

    public AggregationKind getAggregationKind() {
        return aggregationKind;
    }

    public MeasurementUnit getUnit() {
        return unit;
    }

    /** How readings inside one window collapse into the value the thresholds are evaluated against. */
    public enum AggregationKind {
        SUM,
        MAX,
        AVG
    }
}
