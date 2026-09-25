package com.stream.processing.flink;

import com.stream.processing.common.Alert;
import com.stream.processing.common.SensorType;
import com.stream.processing.common.Severity;
import com.stream.processing.common.StationWindowAggregate;
import com.stream.processing.flink.config.PipelineConfig;
import com.stream.processing.flink.ingest.DeadLetterRecord;
import com.stream.processing.flink.testing.CollectingSink;
import com.stream.processing.flink.testing.PerRecordWatermarks;
import com.stream.processing.flink.testing.TestEvents;
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
 * Runs the real topology on a local mini cluster with an in-memory source, so parsing,
 * watermarks and rainfall accumulation are exercised end to end without a broker.
 */
@Timeout(180)
class TelemetryPipelineJobTest {

    private static final Instant T0 = Instant.parse("2026-08-11T05:00:00Z");

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
     * The small STN-RAIN values stay under every window's lowest alerting band, so they only
     * exercise the per-reading aggregate side output. STN-RAIN-HEAVY's single 250mm reading
     * crosses the window=24h EXTREME band (>=204.5mm, see RainfallAlertPolicy) on its own.
     */
    private static List<byte[]> inputPayloads() {
        List<byte[]> payloads = new ArrayList<>();
        payloads.add(TestEvents.readingBytes("STN-RAIN", SensorType.RAINFALL, 3.0d, T0.plusSeconds(60)));
        payloads.add(TestEvents.readingBytes("STN-RAIN", SensorType.RAINFALL, 4.0d, T0.plusSeconds(120)));
        // Two payloads the pipeline must refuse without failing.
        payloads.add(TestEvents.bytes("{\"stationId\": \"STN-RAIN\", this is not json"));
        payloads.add(TestEvents.bytes(
                "{\"sensorType\":\"RAINFALL\",\"value\":9.0,\"eventTime\":\"2026-08-11T05:01:00Z\"}"));
        payloads.add(TestEvents.readingBytes("STN-RAIN", SensorType.RAINFALL, 5.0d, T0.plusSeconds(360)));
        payloads.add(TestEvents.readingBytes("STN-RAIN", SensorType.RAINFALL, 2.0d, T0.plusSeconds(420)));
        payloads.add(TestEvents.readingBytes("STN-RAIN-HEAVY", SensorType.RAINFALL, 250.0d, T0.plusSeconds(500)));
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
    void producesTheExpectedAggregatesAlertsAndDeadLetters() throws Exception {
        runPipeline();

        List<StationWindowAggregate> aggregates = aggregates();
        List<Alert> alerts = alerts();
        List<DeadLetterRecord> deadLetters = CollectingSink.collected(DEAD_LETTERS);

        // One aggregate per reading (no windowing): 4 for STN-RAIN, 1 for STN-RAIN-HEAVY.
        assertThat(aggregates).hasSize(5);

        assertThat(aggregates)
                .filteredOn(aggregate -> "STN-RAIN".equals(aggregate.getStationId()))
                .hasSize(4)
                .allSatisfy(aggregate -> {
                    assertThat(aggregate.getSensorType()).isEqualTo(SensorType.RAINFALL);
                    assertThat(aggregate.getReadingCount()).isEqualTo(1L);
                    // A rainfall aggregate is that single reading's own value, not a window sum.
                    assertThat(aggregate.getSum()).isEqualTo(aggregate.getAggregatedValue());
                    assertThat(aggregate.getWindowEnd()).isEqualTo(aggregate.getWindowStart().plusSeconds(3600));
                    assertThat(aggregate.getStationName()).isEqualTo("STN-RAIN Gauge");
                })
                .extracting(StationWindowAggregate::getAggregatedValue)
                .containsExactlyInAnyOrder(3.0d, 4.0d, 5.0d, 2.0d);

        StationWindowAggregate rainHeavy = aggregates.stream()
                .filter(aggregate -> "STN-RAIN-HEAVY".equals(aggregate.getStationId()))
                .findFirst().orElseThrow();
        assertThat(rainHeavy.getReadingCount()).isEqualTo(1L);
        assertThat(rainHeavy.getAggregatedValue()).isEqualTo(250.0d);
        assertThat(rainHeavy.getWindowEnd()).isEqualTo(T0.plusSeconds(500));

        // One alert: STN-RAIN-HEAVY's 250mm reading crosses the window=24h EXTREME band on its
        // own. The small STN-RAIN readings raise nothing.
        assertThat(alerts).hasSize(1);
        Alert heavyAlert = alerts.get(0);
        assertThat(heavyAlert.getStationId()).isEqualTo("STN-RAIN-HEAVY");
        assertThat(heavyAlert.getSensorType()).isEqualTo(SensorType.RAINFALL);
        assertThat(heavyAlert.getSeverity()).isEqualTo(Severity.EXTREME);
        assertThat(heavyAlert.getObservedValue()).isEqualTo(250.0d);
        // The window=24h extremely_heavy_rainfall lower bound -- see RainfallAlertPolicy.
        assertThat(heavyAlert.getThresholdValue()).isEqualTo(204.5d);
        assertThat(heavyAlert.getMetric()).isEqualTo("RAINFALL_ACCUMULATION_24H");
        assertThat(heavyAlert.getWindowStart()).isEqualTo(T0.plusSeconds(500).minusSeconds(24 * 3600));
        assertThat(heavyAlert.getWindowEnd()).isEqualTo(T0.plusSeconds(500));
        assertThat(heavyAlert.getMessage()).contains("STN-RAIN-HEAVY");

        // Both refused payloads reached the dead-letter branch instead of failing the job.
        assertThat(deadLetters).hasSize(2);
        assertThat(deadLetters).extracting(DeadLetterRecord::getReason)
                .anySatisfy(reason -> assertThat(reason).startsWith("unparseable JSON"))
                .anySatisfy(reason -> assertThat(reason).isEqualTo("failed SensorReading.isValid()"));
        assertThat(deadLetters).allSatisfy(record ->
                assertThat(record.getStage()).isEqualTo("parse-sensor-reading"));
    }

    @Test
    void repeatingTheRunProducesIdenticalAlertIds() throws Exception {
        runPipeline();
        List<String> first = alerts().stream().map(Alert::getAlertId).toList();

        resetCollectors();
        runPipeline();
        List<String> second = alerts().stream().map(Alert::getAlertId).toList();

        // This is what makes at-least-once Kafka sinks safe: a replayed buffer, whether from a
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
        // empty buffer state.
        assertThat(env.getCheckpointConfig().getExternalizedCheckpointRetention())
                .isEqualTo(org.apache.flink.configuration.ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    }
}
