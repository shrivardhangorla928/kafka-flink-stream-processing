package com.stream.processing.flink.testing;

import com.stream.processing.common.SensorReading;
import com.stream.processing.flink.ingest.ReadingWatermarks;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

/**
 * A watermark strategy that advances event time on every record instead of on a timer.
 *
 * <p>The production strategy emits watermarks from a periodic 200 ms timer. In a bounded test the
 * whole job can finish inside one tick, so whether a late record is dropped depends on thread
 * scheduling — exactly the kind of flaky assertion that erodes trust in a suite. Advancing the
 * watermark synchronously with each record makes lateness a function of input order alone, which
 * is deterministic. The production policy itself is covered separately by
 * {@code ReadingWatermarksTest}.</p>
 */
public final class PerRecordWatermarks implements WatermarkGeneratorSupplier<SensorReading> {

    private static final long serialVersionUID = 1L;

    /** Zero tolerance for lateness, so any out-of-order record is late the moment it arrives. */
    public static WatermarkStrategy<SensorReading> strategy() {
        return WatermarkStrategy.forGenerator(new PerRecordWatermarks())
                .withTimestampAssigner(new ReadingWatermarks.EventTimeAssigner());
    }

    @Override
    public WatermarkGenerator<SensorReading> createWatermarkGenerator(Context context) {
        return new HighWaterMarkGenerator();
    }

    /** Emits a watermark equal to the highest timestamp seen so far. */
    private static final class HighWaterMarkGenerator implements WatermarkGenerator<SensorReading> {

        private long highest = Long.MIN_VALUE;

        @Override
        public void onEvent(SensorReading event, long eventTimestamp, WatermarkOutput output) {
            if (eventTimestamp > highest) {
                highest = eventTimestamp;
                output.emitWatermark(new Watermark(highest));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // Everything is emitted on the record path; nothing to do on the timer.
        }
    }
}
