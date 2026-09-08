package com.stream.processing.flink.ingest;

import com.stream.processing.common.SensorReading;
import com.stream.processing.common.SensorType;
import com.stream.processing.flink.config.PipelineConfig;
import com.stream.processing.flink.testing.TestEvents;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.metrics.MetricGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives the production watermark policy directly, rather than inferring it from a running job,
 * so the out-of-orderness allowance is asserted as an exact number.
 */
@Timeout(30)
class ReadingWatermarksTest {

    private static final Instant T0 = Instant.parse("2026-08-11T05:00:00Z");

    private static PipelineConfig config(Map<String, String> args) {
        return PipelineConfig.from(ParameterTool.fromMap(args), name -> null);
    }

    @Test
    void takesEventTimeFromTheStationNotFromIngest() {
        SensorReading reading = TestEvents.reading("STN-1", SensorType.RAINFALL, 3.0d, T0);
        reading.setIngestedAt(T0.plusSeconds(600));

        long timestamp = new ReadingWatermarks.EventTimeAssigner().extractTimestamp(reading, 12345L);

        assertThat(timestamp).isEqualTo(T0.toEpochMilli());
    }

    @Test
    void trailsTheHighestSeenTimestampByTheConfiguredOutOfOrdernessAllowance() {
        WatermarkStrategy<SensorReading> strategy = ReadingWatermarks.forConfig(config(Map.of()));
        WatermarkGenerator<SensorReading> generator =
                strategy.createWatermarkGenerator(new StubContext());
        RecordingOutput output = new RecordingOutput();

        generator.onEvent(null, T0.plusSeconds(120).toEpochMilli(), output);
        // An older record must not pull the watermark backwards.
        generator.onEvent(null, T0.plusSeconds(60).toEpochMilli(), output);
        generator.onPeriodicEmit(output);

        // 30 s default allowance, and Flink's bounded generator emits maxTimestamp - allowance - 1.
        assertThat(output.watermarks).hasSize(1);
        assertThat(output.watermarks.get(0).getTimestamp())
                .isEqualTo(T0.plusSeconds(120).toEpochMilli() - 30_000L - 1L);
    }

    @Test
    void honoursAWiderOutOfOrdernessAllowance() {
        WatermarkStrategy<SensorReading> strategy = ReadingWatermarks.forConfig(
                config(Map.of(PipelineConfig.PARAM_OUT_OF_ORDERNESS_SECONDS, "120")));
        WatermarkGenerator<SensorReading> generator =
                strategy.createWatermarkGenerator(new StubContext());
        RecordingOutput output = new RecordingOutput();

        generator.onEvent(null, T0.plusSeconds(300).toEpochMilli(), output);
        generator.onPeriodicEmit(output);

        assertThat(output.watermarks.get(0).getTimestamp())
                .isEqualTo(T0.plusSeconds(300).toEpochMilli() - 120_000L - 1L);
    }

    @Test
    void wrapsTheStrategyWithIdlenessSoAQuietPartitionCannotStallTheWatermark() {
        WatermarkGenerator<SensorReading> withIdleness = ReadingWatermarks
                .forConfig(config(Map.of()))
                .createWatermarkGenerator(new StubContext());

        assertThat(withIdleness.getClass().getSimpleName()).isEqualTo("WatermarksWithIdleness");
    }

    @Test
    void leavesTheStrategyUnwrappedWhenIdlenessIsDisabled() {
        // Zero would otherwise mean "mark idle immediately", which would let a subtask that is
        // merely between records be treated as permanently quiet.
        WatermarkGenerator<SensorReading> withoutIdleness = ReadingWatermarks
                .forConfig(config(Map.of(PipelineConfig.PARAM_IDLE_TIMEOUT_SECONDS, "0")))
                .createWatermarkGenerator(new StubContext());

        assertThat(withoutIdleness.getClass().getSimpleName()).isEqualTo("BoundedOutOfOrdernessWatermarks");
    }

    @Test
    void neverMarksAnActiveSubtaskIdle() {
        WatermarkGenerator<SensorReading> generator = ReadingWatermarks
                .forConfig(config(Map.of(PipelineConfig.PARAM_IDLE_TIMEOUT_SECONDS, "0")))
                .createWatermarkGenerator(new StubContext());
        RecordingOutput output = new RecordingOutput();

        generator.onEvent(null, T0.toEpochMilli(), output);
        generator.onPeriodicEmit(output);

        assertThat(output.idleMarks).isZero();
    }

    /** Minimal context; the bounded-out-of-orderness generator does not consult it. */
    private static final class StubContext implements WatermarkGeneratorSupplier.Context {

        @Override
        public MetricGroup getMetricGroup() {
            return null;
        }
    }

    /** Captures what the generator emitted, in order. */
    private static final class RecordingOutput implements WatermarkOutput {

        private final List<Watermark> watermarks = new ArrayList<>();
        private int idleMarks;

        @Override
        public void emitWatermark(Watermark watermark) {
            watermarks.add(watermark);
        }

        @Override
        public void markIdle() {
            idleMarks++;
        }

        @Override
        public void markActive() {
            // not asserted
        }
    }
}
