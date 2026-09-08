package com.vassar.aware.flink.ingest;

import com.vassar.aware.common.SensorReading;
import com.vassar.aware.flink.config.PipelineConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

/**
 * The event-time policy for raw telemetry.
 *
 * <p>Time comes from {@link SensorReading#getEventTime()} — when the station took the reading —
 * not from when Kafka received it. Scrapers batch and retry, so ingest order says nothing about
 * observation order, and windowing on ingest time would smear a burst of rainfall across the
 * wrong five minutes.</p>
 *
 * <p>Two knobs matter here and they trade against each other. Bounded out-of-orderness buys
 * tolerance for late scrapes at the cost of delaying every window by that much. Idleness is what
 * stops a quiet partition from pinning the watermark: with three partitions and rain falling in
 * one district, the other two subtasks may see nothing for minutes, and without idleness their
 * stalled watermark would hold back the alert for the district that is actually flooding.</p>
 */
public final class ReadingWatermarks {

    private ReadingWatermarks() {
        // static factory holder
    }

    public static WatermarkStrategy<SensorReading> forConfig(PipelineConfig config) {
        WatermarkStrategy<SensorReading> strategy = WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(config.getOutOfOrderness())
                .withTimestampAssigner(new EventTimeAssigner());
        // Idleness of zero would mark a subtask idle immediately, so treat it as "disabled".
        return config.getIdleTimeoutSeconds() > 0
                ? strategy.withIdleness(config.getIdleTimeout())
                : strategy;
    }

    /** Reads the station's observation time as the record timestamp. */
    public static final class EventTimeAssigner implements SerializableTimestampAssigner<SensorReading> {

        private static final long serialVersionUID = 1L;

        @Override
        public long extractTimestamp(SensorReading element, long recordTimestamp) {
            // Parsing has already rejected readings without an event time, so this cannot be null
            // for anything reaching the assigner.
            return element.getEventTime().toEpochMilli();
        }
    }
}
