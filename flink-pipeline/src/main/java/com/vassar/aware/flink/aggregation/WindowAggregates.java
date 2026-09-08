package com.vassar.aware.flink.aggregation;

import com.vassar.aware.common.SensorType;
import com.vassar.aware.common.StationWindowAggregate;

import java.time.Instant;

/**
 * Turns a finished accumulator into the published {@link StationWindowAggregate}.
 *
 * <p>Free of Flink types so the rule that actually matters here — which of sum, max or average
 * becomes {@code aggregatedValue} — can be tested directly. That choice is the hinge of the whole
 * alerting behaviour: rainfall thresholds are about how much fell in the window, whereas reservoir
 * and river thresholds are about the worst level reached, and mixing the two up produces a job
 * that runs perfectly and alerts on the wrong thing.</p>
 */
public final class WindowAggregates {

    private WindowAggregates() {
        // static holder
    }

    public static StationWindowAggregate toAggregate(StationWindowAccumulator accumulator,
                                                     Instant windowStart,
                                                     Instant windowEnd,
                                                     Instant computedAt) {
        StationWindowAggregate aggregate = new StationWindowAggregate();
        aggregate.setStationId(accumulator.getStationId());
        aggregate.setStationName(accumulator.getStationName());
        aggregate.setDistrictId(accumulator.getDistrictId());
        aggregate.setSensorType(accumulator.getSensorType());
        aggregate.setUnit(accumulator.getUnit() != null ? accumulator.getUnit()
                : (accumulator.getSensorType() == null ? null : accumulator.getSensorType().getUnit()));
        aggregate.setWindowStart(windowStart);
        aggregate.setWindowEnd(windowEnd);
        aggregate.setReadingCount(accumulator.getCount());
        aggregate.setSum(accumulator.getSum());
        aggregate.setMin(accumulator.minOrZero());
        aggregate.setMax(accumulator.maxOrZero());
        aggregate.setAvg(accumulator.average());
        aggregate.setAggregatedValue(aggregatedValue(accumulator));
        aggregate.setLatitude(accumulator.getLatitude());
        aggregate.setLongitude(accumulator.getLongitude());
        aggregate.setComputedAt(computedAt);
        return aggregate;
    }

    /** The single number thresholds are applied to, chosen by the sensor type's aggregation kind. */
    public static double aggregatedValue(StationWindowAccumulator accumulator) {
        SensorType sensorType = accumulator.getSensorType();
        if (sensorType == null) {
            // Nothing sensible to reduce to; sum is the least surprising fallback and the
            // aggregate still carries all four reductions for whoever investigates.
            return accumulator.getSum();
        }
        return switch (sensorType.getAggregationKind()) {
            case SUM -> accumulator.getSum();
            case MAX -> accumulator.maxOrZero();
            case AVG -> accumulator.average();
        };
    }
}
