package com.vassar.aware.flink.aggregation;

import com.vassar.aware.common.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Folds every reading of a window into a fixed-size accumulator.
 *
 * <p>An {@code AggregateFunction} rather than a plain window function because it reduces
 * incrementally: state stays at one accumulator per station per window regardless of how often a
 * station reports, and the window fires without a burst of work at the trigger.</p>
 *
 * <p>All four reductions (count, sum, min, max) are kept even though only one of them becomes the
 * aggregated value. They cost nothing extra to maintain and the aggregated telemetry topic is
 * what the Grafana dashboards read, where the spread inside a window is the interesting part.</p>
 */
public final class ReadingAggregateFunction
        implements AggregateFunction<SensorReading, StationWindowAccumulator, StationWindowAccumulator> {

    private static final long serialVersionUID = 1L;

    @Override
    public StationWindowAccumulator createAccumulator() {
        return new StationWindowAccumulator();
    }

    @Override
    public StationWindowAccumulator add(SensorReading value, StationWindowAccumulator accumulator) {
        if (value == null) {
            return accumulator;
        }
        copyMetadata(value, accumulator);
        accumulator.setCount(accumulator.getCount() + 1L);
        accumulator.setSum(accumulator.getSum() + value.getValue());
        accumulator.setMin(Math.min(accumulator.getMin(), value.getValue()));
        accumulator.setMax(Math.max(accumulator.getMax(), value.getValue()));
        return accumulator;
    }

    @Override
    public StationWindowAccumulator getResult(StationWindowAccumulator accumulator) {
        // Detached so the window function cannot observe later mutations of live state.
        return accumulator.copy();
    }

    @Override
    public StationWindowAccumulator merge(StationWindowAccumulator a, StationWindowAccumulator b) {
        StationWindowAccumulator merged = a.copy();
        merged.setCount(a.getCount() + b.getCount());
        merged.setSum(a.getSum() + b.getSum());
        merged.setMin(Math.min(a.getMin(), b.getMin()));
        merged.setMax(Math.max(a.getMax(), b.getMax()));
        // The side that actually saw readings owns the metadata; an empty accumulator has none.
        if (a.isEmpty() && !b.isEmpty()) {
            copyMetadata(b, merged);
        }
        return merged;
    }

    private static void copyMetadata(SensorReading from, StationWindowAccumulator to) {
        to.setStationId(from.getStationId());
        to.setSensorType(from.getSensorType());
        // The reading may omit the unit; the sensor type is the authority on it.
        to.setUnit(from.getUnit() != null ? from.getUnit()
                : (from.getSensorType() == null ? null : from.getSensorType().getUnit()));
        if (from.getStationName() != null) {
            to.setStationName(from.getStationName());
        }
        if (from.getDistrictId() != null) {
            to.setDistrictId(from.getDistrictId());
        }
        if (from.getLatitude() != 0.0d || from.getLongitude() != 0.0d) {
            to.setLatitude(from.getLatitude());
            to.setLongitude(from.getLongitude());
        }
    }

    private static void copyMetadata(StationWindowAccumulator from, StationWindowAccumulator to) {
        to.setStationId(from.getStationId());
        to.setStationName(from.getStationName());
        to.setDistrictId(from.getDistrictId());
        to.setSensorType(from.getSensorType());
        to.setUnit(from.getUnit());
        to.setLatitude(from.getLatitude());
        to.setLongitude(from.getLongitude());
    }
}
