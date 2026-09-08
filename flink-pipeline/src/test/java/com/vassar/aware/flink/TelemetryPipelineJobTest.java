package com.vassar.aware.flink;

import com.vassar.aware.common.Alert;
import com.vassar.aware.common.SensorType;
import com.vassar.aware.common.Severity;
import com.vassar.aware.common.StationWindowAggregate;
import com.vassar.aware.flink.config.PipelineConfig;
import com.vassar.aware.flink.ingest.DeadLetterRecord;
import com.vassar.aware.flink.testing.CollectingSink;
import com.vassar.aware.flink.testing.PerRecordWatermarks;
import com.vassar.aware.flink.testing.TestEvents;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs the real topology on a local mini cluster with an in-memory source, so the wiring between
 * parsing, watermarks, windowing and threshold evaluation is exercised end to end without a
 * broker.
 *
 * <p>Event time is driven by {@link PerRecordWatermarks} rather than the production periodic
 * strategy. That is the only way to make lateness deterministic in a bounded job: with the
 * production 200 ms watermark timer, whether the late record lands before or after the window
 * fires depends on thread scheduling.</p>
 */
@Timeout(180)
class TelemetryPipelineJobTest {

    /** Aligned to a five-minute boundary, so the windows are exactly the ones being asserted. */
    private static final Instant T0 = Instant.parse("2026-08-11T05:00:00Z");
    private static final Instant WINDOW_A_END = T0.plusSeconds(300);
    private static final Instant WINDOW_B_END = T0.plusSeconds(600);

    private static final String AGGREGATES = "job-test-aggregates";
    private static final String ALERTS = "job-test-alerts";
    private static final String DEAD_LETTERS = "job-test-dead-letters";

    private static PipelineConfig defaultConfig() {
        return PipelineConfig.from(ParameterTool.fromMap(Map.of()), name -> null);
    }

    @BeforeEach
    void resetCollectors() {
        CollectingSink.reset(AGGREGATES);
        CollectingSink.reset(ALERTS);
        CollectingSink.reset(DEAD_LETTERS);
    }

    /**
     * The payloads, in the order the source emits them. Order matters: it is what advances the
     * watermark, and therefore what makes the final record late.
     */
    private static List<byte[]> inputPayloads() {
        List<byte[]> payloads = new ArrayList<>();
        // Window A = [05:00, 05:05). Rainfall sums to 22 mm, which clears the 15 mm WARNING band.
        payloads.add(TestEvents.readingBytes("STN-RAIN", SensorType.RAINFALL, 10.0d, T0.plusSeconds(60)));
        payloads.add(TestEvents.readingBytes("STN-RAIN", SensorType.RAINFALL, 12.0d, T0.plusSeconds(120)));
        // Window A for a river gauge. Max is 5 m, below the 8 m WARNING band, so it stays quiet.
        payloads.add(TestEvents.readingBytes("STN-RIVER", SensorType.RIVER_LEVEL, 3.0d, T0.plusSeconds(60)));
        payloads.add(TestEvents.readingBytes("STN-RIVER", SensorType.RIVER_LEVEL, 5.0d, T0.plusSeconds(120)));
        // Two payloads the pipeline must refuse without failing.
        payloads.add(TestEvents.bytes("{\"stationId\": \"STN-RAIN\", this is not json"));
        payloads.add(TestEvents.bytes(
                "{\"sensorType\":\"RAINFALL\",\"value\":9.0,\"eventTime\":\"2026-08-11T05:01:00Z\"}"));
        // Window B = [05:05, 05:10). Rainfall sums to 35 mm, which clears the 30 mm SEVERE band.
        // These also push the watermark past the end of window A and fire it.
        payloads.add(TestEvents.readingBytes("STN-RAIN", SensorType.RAINFALL, 20.0d, T0.plusSeconds(360)));
        payloads.add(TestEvents.readingBytes("STN-RAIN", SensorType.RAINFALL, 15.0d, T0.plusSeconds(420)));
        // Late: belongs to window A, which has already fired. Must be dropped, and its 100 mm
        // must not appear in any aggregate.
        payloads.add(TestEvents.readingBytes("STN-RAIN", SensorType.RAINFALL, 100.0d, T0.plusSeconds(180)));
        return payloads;
    }

    private static void runPipeline() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // One subtask end to end, so the order the payloads are declared in is the order the
        // watermark advances in.
        env.setParallelism(1);

        DataStream<byte[]> source = env.fromData(
                inputPayloads(), PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);

        TelemetryPipelineJob.PipelineStreams streams = TelemetryPipelineJob.buildTopology(
                source, defaultConfig(), PerRecordWatermarks.strategy());

        streams.aggregates().sinkTo(new CollectingSink<>(AGGREGATES));
        streams.alerts().sinkTo(new CollectingSink<>(ALERTS));
        streams.deadLetters().sinkTo(new CollectingSink<>(DEAD_LETTERS));

        env.execute("telemetry-pipeline-test");
    }

    private static List<StationWindowAggregate> aggregates() {
        List<StationWindowAggregate> collected = new ArrayList<>(CollectingSink.collected(AGGREGATES));
        collected.sort(Comparator.comparing(StationWindowAggregate::getWindowStart)
                .thenComparing(StationWindowAggregate::getStationId));
        return collected;
    }

    private static List<Alert> alerts() {
        List<Alert> collected = new ArrayList<>(CollectingSink.collected(ALERTS));
        collected.sort(Comparator.comparing(Alert::getWindowStart).thenComparing(Alert::getStationId));
        return collected;
    }

    @Test
    void producesTheExpectedAggregatesAlertsAndDeadLettersAcrossTwoWindows() throws Exception {
        runPipeline();

        List<StationWindowAggregate> aggregates = aggregates();
        List<Alert> alerts = alerts();
        List<DeadLetterRecord> deadLetters = CollectingSink.collected(DEAD_LETTERS);

        // Three windows fired: two stations in window A, one station in window B.
        assertThat(aggregates).hasSize(3);

        StationWindowAggregate rainWindowA = aggregates.get(0);
        assertThat(rainWindowA.getStationId()).isEqualTo("STN-RAIN");
        assertThat(rainWindowA.getWindowStart()).isEqualTo(T0);
        assertThat(rainWindowA.getWindowEnd()).isEqualTo(WINDOW_A_END);
        assertThat(rainWindowA.getReadingCount()).isEqualTo(2L);
        assertThat(rainWindowA.getSum()).isEqualTo(22.0d);
        assertThat(rainWindowA.getMin()).isEqualTo(10.0d);
        assertThat(rainWindowA.getMax()).isEqualTo(12.0d);
        assertThat(rainWindowA.getAvg()).isEqualTo(11.0d);
        // Rainfall aggregates by sum.
        assertThat(rainWindowA.getAggregatedValue()).isEqualTo(22.0d);
        assertThat(rainWindowA.getStationName()).isEqualTo("STN-RAIN Gauge");
        assertThat(rainWindowA.getComputedAt()).isNotNull();

        StationWindowAggregate riverWindowA = aggregates.get(1);
        assertThat(riverWindowA.getStationId()).isEqualTo("STN-RIVER");
        assertThat(riverWindowA.getWindowEnd()).isEqualTo(WINDOW_A_END);
        assertThat(riverWindowA.getSum()).isEqualTo(8.0d);
        // River level aggregates by max, not sum: 5 m, not 8 m.
        assertThat(riverWindowA.getAggregatedValue()).isEqualTo(5.0d);

        StationWindowAggregate rainWindowB = aggregates.get(2);
        assertThat(rainWindowB.getStationId()).isEqualTo("STN-RAIN");
        assertThat(rainWindowB.getWindowStart()).isEqualTo(WINDOW_A_END);
        assertThat(rainWindowB.getWindowEnd()).isEqualTo(WINDOW_B_END);
        assertThat(rainWindowB.getReadingCount()).isEqualTo(2L);
        assertThat(rainWindowB.getAggregatedValue()).isEqualTo(35.0d);

        // Two alerts: the calm river window raises nothing.
        assertThat(alerts).hasSize(2);
        assertThat(alerts.get(0).getStationId()).isEqualTo("STN-RAIN");
        assertThat(alerts.get(0).getSeverity()).isEqualTo(Severity.WARNING);
        assertThat(alerts.get(0).getObservedValue()).isEqualTo(22.0d);
        assertThat(alerts.get(0).getThresholdValue()).isEqualTo(PipelineConfig.DEFAULT_RAINFALL_WARNING);
        assertThat(alerts.get(0).getMetric()).isEqualTo(PipelineConfig.METRIC_RAINFALL);
        assertThat(alerts.get(0).getWindowEnd()).isEqualTo(WINDOW_A_END);

        assertThat(alerts.get(1).getSeverity()).isEqualTo(Severity.SEVERE);
        assertThat(alerts.get(1).getObservedValue()).isEqualTo(35.0d);
        assertThat(alerts.get(1).getWindowStart()).isEqualTo(WINDOW_A_END);

        assertThat(alerts).extracting(Alert::getAlertId).doesNotHaveDuplicates();
        assertThat(alerts).extracting(Alert::getMessage).allSatisfy(message ->
                assertThat(message).contains("STN-RAIN"));

        // Both refused payloads reached the dead-letter branch instead of failing the job.
        assertThat(deadLetters).hasSize(2);
        assertThat(deadLetters).extracting(DeadLetterRecord::getReason)
                .anySatisfy(reason -> assertThat(reason).startsWith("unparseable JSON"))
                .anySatisfy(reason -> assertThat(reason).isEqualTo("failed SensorReading.isValid()"));
        assertThat(deadLetters).allSatisfy(record ->
                assertThat(record.getStage()).isEqualTo("parse-sensor-reading"));
    }

    @Test
    void dropsARecordThatArrivesAfterItsWindowHasFired() throws Exception {
        runPipeline();

        // The late reading is 100 mm. Had it been admitted, window A would have summed to 122 mm
        // and raised EXTREME rather than WARNING.
        assertThat(aggregates())
                .filteredOn(aggregate -> "STN-RAIN".equals(aggregate.getStationId())
                        && T0.equals(aggregate.getWindowStart()))
                .singleElement()
                .satisfies(aggregate -> {
                    assertThat(aggregate.getReadingCount()).isEqualTo(2L);
                    assertThat(aggregate.getAggregatedValue()).isEqualTo(22.0d);
                });
        assertThat(alerts()).extracting(Alert::getSeverity).doesNotContain(Severity.EXTREME);
    }

    @Test
    void repeatingTheRunProducesIdenticalAlertIds() throws Exception {
        runPipeline();
        List<String> first = alerts().stream().map(Alert::getAlertId).toList();

        resetCollectors();
        runPipeline();
        List<String> second = alerts().stream().map(Alert::getAlertId).toList();

        // This is what makes at-least-once Kafka sinks safe: a replayed window, whether from a
        // checkpoint restore or a resubmission, yields the same id for the alert store to upsert.
        assertThat(second).isEqualTo(first);
    }

    @Test
    void configuresTheRuntimeForRestoreAndUpgrade() {
        StreamExecutionEnvironment env = TelemetryPipelineJob.createEnvironment(defaultConfig());

        assertThat(env.getParallelism()).isEqualTo(PipelineConfig.DEFAULT_PARALLELISM);
        assertThat(env.getCheckpointConfig().isCheckpointingEnabled()).isTrue();
        assertThat(env.getCheckpointConfig().getCheckpointInterval())
                .isEqualTo(PipelineConfig.DEFAULT_CHECKPOINT_INTERVAL_MS);
        assertThat(env.getCheckpointConfig().getCheckpointingConsistencyMode())
                .isEqualTo(org.apache.flink.core.execution.CheckpointingMode.EXACTLY_ONCE);
        assertThat(env.getCheckpointConfig().getMinPauseBetweenCheckpoints()).isEqualTo(10_000L);
        assertThat(env.getCheckpointConfig().getCheckpointTimeout()).isEqualTo(120_000L);
        assertThat(env.getCheckpointConfig().getTolerableCheckpointFailureNumber()).isEqualTo(3);
        // Retained on cancellation, so a planned stop can be resumed rather than restarting with
        // empty window state.
        assertThat(env.getCheckpointConfig().getExternalizedCheckpointRetention())
                .isEqualTo(org.apache.flink.configuration.ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    }

    @Test
    void honoursAConfiguredWindowSize() throws Exception {
        PipelineConfig tenMinuteWindows = PipelineConfig.from(
                ParameterTool.fromMap(Map.of(PipelineConfig.PARAM_WINDOW_SIZE_MINUTES, "10")),
                name -> null);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<byte[]> source = env.fromData(
                inputPayloads(), PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
        TelemetryPipelineJob.PipelineStreams streams =
                TelemetryPipelineJob.buildTopology(source, tenMinuteWindows, PerRecordWatermarks.strategy());
        streams.aggregates().sinkTo(new CollectingSink<>(AGGREGATES));
        streams.alerts().sinkTo(new CollectingSink<>(ALERTS));
        streams.deadLetters().sinkTo(new CollectingSink<>(DEAD_LETTERS));
        env.execute("telemetry-pipeline-wide-window-test");

        // One ten-minute window now covers everything, so the two rainfall windows collapse into
        // one and the late record is no longer late.
        assertThat(aggregates()).allSatisfy(aggregate -> {
            assertThat(aggregate.getWindowStart()).isEqualTo(T0);
            assertThat(aggregate.getWindowEnd()).isEqualTo(WINDOW_B_END);
        });
        assertThat(aggregates())
                .filteredOn(aggregate -> "STN-RAIN".equals(aggregate.getStationId()))
                .singleElement()
                .satisfies(aggregate -> {
                    assertThat(aggregate.getReadingCount()).isEqualTo(5L);
                    assertThat(aggregate.getAggregatedValue()).isEqualTo(157.0d);
                });
        assertThat(alerts()).extracting(Alert::getSeverity).containsExactly(Severity.EXTREME);
    }
}
