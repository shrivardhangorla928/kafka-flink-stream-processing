package com.vassar.aware.flink.aggregation;

import com.vassar.aware.common.MeasurementUnit;
import com.vassar.aware.common.SensorReading;
import com.vassar.aware.common.SensorType;
import com.vassar.aware.common.StationWindowAggregate;
import com.vassar.aware.flink.testing.TestEvents;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/** Verifies the incremental reduction that backs every window. */
@Timeout(30)
class ReadingAggregateFunctionTest {

    private static final Instant T0 = Instant.parse("2026-08-11T05:00:00Z");

    private final ReadingAggregateFunction function = new ReadingAggregateFunction();

    private StationWindowAccumulator accumulate(SensorType sensorType, double... values) {
        StationWindowAccumulator accumulator = function.createAccumulator();
        for (int i = 0; i < values.length; i++) {
            accumulator = function.add(
                    TestEvents.reading("STN-1", sensorType, values[i], T0.plusSeconds(30L * i)),
                    accumulator);
        }
        return accumulator;
    }

    @Test
    void startsEmptyWithNeutralIdentities() {
        StationWindowAccumulator empty = function.createAccumulator();

        assertThat(empty.isEmpty()).isTrue();
        assertThat(empty.getCount()).isZero();
        assertThat(empty.getSum()).isZero();
        // The identities are infinite so the first reading always replaces them, but the
        // published view must not leak an infinity that JSON cannot represent.
        assertThat(empty.getMin()).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(empty.getMax()).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(empty.minOrZero()).isZero();
        assertThat(empty.maxOrZero()).isZero();
        assertThat(empty.average()).isZero();
    }

    @Test
    void reducesCountSumMinAndMax() {
        StationWindowAccumulator accumulator = accumulate(SensorType.RAINFALL, 4.0d, 11.5d, 0.5d, 6.0d);

        assertThat(accumulator.getCount()).isEqualTo(4L);
        assertThat(accumulator.getSum()).isEqualTo(22.0d);
        assertThat(accumulator.getMin()).isEqualTo(0.5d);
        assertThat(accumulator.getMax()).isEqualTo(11.5d);
        assertThat(accumulator.average()).isCloseTo(5.5d, within(1e-9d));
    }

    @Test
    void handlesASingleReading() {
        StationWindowAccumulator accumulator = accumulate(SensorType.RIVER_LEVEL, 7.25d);

        assertThat(accumulator.getCount()).isEqualTo(1L);
        assertThat(accumulator.getSum()).isEqualTo(7.25d);
        assertThat(accumulator.getMin()).isEqualTo(7.25d);
        assertThat(accumulator.getMax()).isEqualTo(7.25d);
        assertThat(accumulator.average()).isEqualTo(7.25d);
    }

    @Test
    void carriesStationMetadataForwardFromTheReadings() {
        StationWindowAccumulator accumulator = accumulate(SensorType.RESERVOIR_LEVEL, 80.0d, 90.0d);

        assertThat(accumulator.getStationId()).isEqualTo("STN-1");
        assertThat(accumulator.getStationName()).isEqualTo("STN-1 Gauge");
        assertThat(accumulator.getDistrictId()).isEqualTo("D-STN-1");
        assertThat(accumulator.getSensorType()).isEqualTo(SensorType.RESERVOIR_LEVEL);
        assertThat(accumulator.getUnit()).isEqualTo(MeasurementUnit.PERCENT);
        assertThat(accumulator.getLatitude()).isEqualTo(16.5d);
        assertThat(accumulator.getLongitude()).isEqualTo(80.6d);
    }

    @Test
    void fallsBackToTheSensorTypeUnitWhenTheReadingOmitsIt() {
        SensorReading unitless = TestEvents.reading("STN-1", SensorType.RIVER_LEVEL, 3.0d, T0);
        unitless.setUnit(null);

        StationWindowAccumulator accumulator =
                function.add(unitless, function.createAccumulator());

        assertThat(accumulator.getUnit()).isEqualTo(MeasurementUnit.METRE);
    }

    @Test
    void ignoresANullReadingRatherThanFailingTheWindow() {
        StationWindowAccumulator accumulator = accumulate(SensorType.RAINFALL, 5.0d);

        StationWindowAccumulator afterNull = function.add(null, accumulator);

        assertThat(afterNull.getCount()).isEqualTo(1L);
        assertThat(afterNull.getSum()).isEqualTo(5.0d);
    }

    @Test
    void detachesTheResultFromLiveState() {
        StationWindowAccumulator live = accumulate(SensorType.RAINFALL, 5.0d);

        StationWindowAccumulator result = function.getResult(live);
        function.add(TestEvents.reading("STN-1", SensorType.RAINFALL, 100.0d, T0), live);

        assertThat(result).isNotSameAs(live);
        assertThat(result.getSum()).isEqualTo(5.0d);
        assertThat(live.getSum()).isEqualTo(105.0d);
    }

    @Test
    void mergesTwoPartialAccumulators() {
        StationWindowAccumulator left = accumulate(SensorType.RAINFALL, 4.0d, 11.5d);
        StationWindowAccumulator right = accumulate(SensorType.RAINFALL, 0.5d, 6.0d);

        StationWindowAccumulator merged = function.merge(left, right);

        assertThat(merged.getCount()).isEqualTo(4L);
        assertThat(merged.getSum()).isEqualTo(22.0d);
        assertThat(merged.getMin()).isEqualTo(0.5d);
        assertThat(merged.getMax()).isEqualTo(11.5d);
        assertThat(merged.average()).isCloseTo(5.5d, within(1e-9d));
        assertThat(merged.getStationId()).isEqualTo("STN-1");
    }

    @Test
    void mergeLeavesItsOperandsUntouched() {
        StationWindowAccumulator left = accumulate(SensorType.RAINFALL, 4.0d);
        StationWindowAccumulator right = accumulate(SensorType.RAINFALL, 6.0d);

        function.merge(left, right);

        assertThat(left.getCount()).isEqualTo(1L);
        assertThat(left.getSum()).isEqualTo(4.0d);
        assertThat(right.getCount()).isEqualTo(1L);
        assertThat(right.getSum()).isEqualTo(6.0d);
    }

    @Test
    void mergeIsCommutative() {
        StationWindowAccumulator left = accumulate(SensorType.RIVER_LEVEL, 2.0d, 9.0d);
        StationWindowAccumulator right = accumulate(SensorType.RIVER_LEVEL, 5.0d);

        StationWindowAccumulator leftFirst = function.merge(left, right);
        StationWindowAccumulator rightFirst = function.merge(right, left);

        assertThat(leftFirst.getCount()).isEqualTo(rightFirst.getCount());
        assertThat(leftFirst.getSum()).isEqualTo(rightFirst.getSum());
        assertThat(leftFirst.getMin()).isEqualTo(rightFirst.getMin());
        assertThat(leftFirst.getMax()).isEqualTo(rightFirst.getMax());
    }

    @Test
    void mergingAnEmptyAccumulatorIsTheIdentity() {
        StationWindowAccumulator populated = accumulate(SensorType.RAINFALL, 4.0d, 11.5d);
        StationWindowAccumulator empty = function.createAccumulator();

        StationWindowAccumulator emptyOnRight = function.merge(populated, empty);
        StationWindowAccumulator emptyOnLeft = function.merge(empty, populated);

        assertThat(emptyOnRight.getCount()).isEqualTo(2L);
        assertThat(emptyOnRight.getSum()).isEqualTo(15.5d);
        assertThat(emptyOnRight.getMin()).isEqualTo(4.0d);
        assertThat(emptyOnRight.getMax()).isEqualTo(11.5d);

        // The side that actually saw readings has to win the metadata, otherwise merging in an
        // empty accumulator would silently blank out the station name.
        assertThat(emptyOnLeft.getStationId()).isEqualTo("STN-1");
        assertThat(emptyOnLeft.getSensorType()).isEqualTo(SensorType.RAINFALL);
        assertThat(emptyOnLeft.getSum()).isEqualTo(15.5d);
    }

    @Test
    void mergingTwoEmptyAccumulatorsStaysEmpty() {
        StationWindowAccumulator merged =
                function.merge(function.createAccumulator(), function.createAccumulator());

        assertThat(merged.isEmpty()).isTrue();
        assertThat(merged.minOrZero()).isZero();
        assertThat(merged.maxOrZero()).isZero();
    }

    @Test
    void copyIsIndependentOfTheOriginal() {
        StationWindowAccumulator original = accumulate(SensorType.RAINFALL, 3.0d);

        StationWindowAccumulator copy = original.copy();
        original.setSum(999.0d);

        assertThat(copy.getSum()).isEqualTo(3.0d);
        assertThat(copy.getStationId()).isEqualTo(original.getStationId());
        assertThat(copy.getSensorType()).isEqualTo(original.getSensorType());
    }

    @Test
    void publishesRainfallAsTheWindowSum() {
        StationWindowAccumulator accumulator = accumulate(SensorType.RAINFALL, 4.0d, 11.5d, 0.5d, 6.0d);

        StationWindowAggregate aggregate = WindowAggregates.toAggregate(
                accumulator, T0, T0.plusSeconds(300), T0.plusSeconds(301));

        assertThat(aggregate.getAggregatedValue()).isEqualTo(22.0d);
        assertThat(aggregate.getSum()).isEqualTo(22.0d);
        assertThat(aggregate.getMax()).isEqualTo(11.5d);
    }

    @Test
    void publishesLevelsAsTheWindowMaximum() {
        StationWindowAccumulator reservoir = accumulate(SensorType.RESERVOIR_LEVEL, 80.0d, 96.0d, 91.0d);
        StationWindowAccumulator river = accumulate(SensorType.RIVER_LEVEL, 2.0d, 9.5d, 7.0d);

        assertThat(WindowAggregates.aggregatedValue(reservoir)).isEqualTo(96.0d);
        assertThat(WindowAggregates.aggregatedValue(river)).isEqualTo(9.5d);
    }
}
