package com.stream.processing.flink;

import com.stream.processing.common.Alert;
import com.stream.processing.common.SensorReading;
import com.stream.processing.common.StationWindowAggregate;
import com.stream.processing.common.Topics;
import com.stream.processing.flink.config.PipelineConfig;
import com.stream.processing.flink.ingest.DeadLetterRecord;
import com.stream.processing.flink.ingest.ParseSensorReadingFunction;
import com.stream.processing.flink.ingest.ReadingWatermarks;
import com.stream.processing.flink.ingest.StationIdKeySelector;
import com.stream.processing.flink.kafka.KafkaConnectors;
import com.stream.processing.flink.rainfall.RainfallAccumulationFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;

/**
 * The scrape-to-alert stream job: raw rainfall telemetry in, rolling 24h aggregates and alerts out.
 *
 * <pre>
 * telemetry.raw.v1 -> parse -> watermarks -> keyBy(station) -> rolling 24h accumulation
 *                        |                                            |         |
 *                        v                                            v         v
 *                telemetry.dlq.v1                    telemetry.aggregated.v1  alerts.generated.v1
 * </pre>
 *
 * <p>The topology is built by {@link #buildTopology} rather than inline in {@code main} so the
 * whole graph can be exercised against an in-memory source in a test. {@code main} is only
 * responsible for configuration, the execution environment and attaching real Kafka connectors.</p>
 *
 * <p>Every operator carries an explicit {@code uid} so a savepoint can be restored across code
 * changes without every operator id shifting.</p>
 */
public final class TelemetryPipelineJob {

    /** Name shown in the Flink dashboard and used by the deployment scripts to find the job. */
    public static final String JOB_NAME = "stream-telemetry-pipeline";

    private static final Logger LOG = LoggerFactory.getLogger(TelemetryPipelineJob.class);

    private static final int RESTART_ATTEMPTS = 3;
    private static final Duration RESTART_DELAY = Duration.ofSeconds(10);
    private static final long MIN_PAUSE_BETWEEN_CHECKPOINTS_MS = 10_000L;
    private static final long CHECKPOINT_TIMEOUT_MS = 120_000L;
    private static final int TOLERABLE_CHECKPOINT_FAILURES = 3;

    private TelemetryPipelineJob() {
        // entry point holder
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        PipelineConfig config = PipelineConfig.from(parameters, System::getenv);
        LOG.info("Starting {} with {}", JOB_NAME, config);

        StreamExecutionEnvironment env = createEnvironment(config);
        // Surfaces the resolved arguments in the Flink dashboard, so what a running job was
        // actually given is recoverable without hunting down the submitting shell.
        env.getConfig().setGlobalJobParameters(parameters);

        DataStream<byte[]> rawPayloads = env
                .fromSource(
                        KafkaConnectors.rawTelemetrySource(config),
                        // Watermarks are assigned after parsing, once the event time is readable.
                        WatermarkStrategy.noWatermarks(),
                        "kafka-source-" + Topics.RAW_TELEMETRY)
                .name("kafka-source-raw-telemetry")
                .uid("kafka-source-raw-telemetry");

        PipelineStreams streams = buildTopology(rawPayloads, config);

        streams.aggregates()
                .sinkTo(KafkaConnectors.aggregateSink(config))
                .name("kafka-sink-aggregated-telemetry")
                .uid("kafka-sink-aggregated-telemetry");

        streams.alerts()
                .sinkTo(KafkaConnectors.alertSink(config))
                .name("kafka-sink-generated-alerts")
                .uid("kafka-sink-generated-alerts");

        streams.deadLetters()
                .sinkTo(KafkaConnectors.deadLetterSink(config))
                .name("kafka-sink-dead-letter")
                .uid("kafka-sink-dead-letter");

        env.execute(JOB_NAME);
    }

    /**
     * Configures the runtime for a job that is expected to be restored, rescaled and upgraded.
     *
     * <p>Exactly-once checkpointing keeps the keyed rainfall buffer state consistent on restore.
     * The sinks are separately at-least-once — see {@link KafkaConnectors}.</p>
     */
    public static StreamExecutionEnvironment createEnvironment(PipelineConfig config) {
        Objects.requireNonNull(config, "config");

        // The restart strategy is set through configuration because the RestartStrategies
        // builders on ExecutionConfig are deprecated in the 1.20 line.
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY,
                RestartStrategyOptions.RestartStrategyType.FIXED_DELAY.getMainValue());
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, RESTART_ATTEMPTS);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, RESTART_DELAY);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        // Matches the three partitions of the raw telemetry topic: one source subtask per
        // partition, so no subtask sits idle and none multiplexes two partitions' watermarks.
        env.setParallelism(config.getParallelism());
        env.enableCheckpointing(config.getCheckpointIntervalMs(), CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpoints = env.getCheckpointConfig();
        // A floor on the gap between checkpoints, so a slow checkpoint cannot starve record
        // processing by immediately triggering the next one.
        checkpoints.setMinPauseBetweenCheckpoints(MIN_PAUSE_BETWEEN_CHECKPOINTS_MS);
        checkpoints.setCheckpointTimeout(CHECKPOINT_TIMEOUT_MS);
        // Tolerating a few failures keeps a transient storage blip from restarting the job.
        checkpoints.setTolerableCheckpointFailureNumber(TOLERABLE_CHECKPOINT_FAILURES);
        // Retained on cancellation so a planned stop can be resumed from the last checkpoint;
        // deleting them would make every deploy start from an empty buffer state.
        checkpoints.setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        return env;
    }

    /** Builds the topology with the production watermark policy derived from configuration. */
    public static PipelineStreams buildTopology(DataStream<byte[]> rawPayloads, PipelineConfig config) {
        return buildTopology(rawPayloads, config, ReadingWatermarks.forConfig(config));
    }

    /**
     * Builds the topology against an explicit watermark strategy, so a test can drive event time
     * deterministically instead of depending on the periodic watermark timer.
     */
    public static PipelineStreams buildTopology(DataStream<byte[]> rawPayloads,
                                                PipelineConfig config,
                                                WatermarkStrategy<SensorReading> watermarkStrategy) {
        Objects.requireNonNull(rawPayloads, "rawPayloads");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(watermarkStrategy, "watermarkStrategy");

        SingleOutputStreamOperator<SensorReading> parsed = rawPayloads
                .process(new ParseSensorReadingFunction())
                .name("parse-sensor-reading")
                .uid("parse-sensor-reading");

        DataStream<DeadLetterRecord> deadLetters =
                parsed.getSideOutput(ParseSensorReadingFunction.DEAD_LETTER_TAG);

        DataStream<SensorReading> timestamped = parsed
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("assign-event-time-watermarks")
                .uid("assign-event-time-watermarks");

        SingleOutputStreamOperator<Alert> alerts = timestamped
                .keyBy(new StationIdKeySelector())
                .process(new RainfallAccumulationFunction())
                .name("rainfall-accumulate")
                .uid("rainfall-accumulate");

        DataStream<StationWindowAggregate> aggregates =
                alerts.getSideOutput(RainfallAccumulationFunction.HOURLY_AGGREGATE_TAG);

        return new PipelineStreams(aggregates, alerts, deadLetters);
    }

    /**
     * The three streams the topology exposes, so callers decide where they go: Kafka in
     * production, collecting sinks in tests.
     */
    public record PipelineStreams(DataStream<StationWindowAggregate> aggregates,
                                  DataStream<Alert> alerts,
                                  DataStream<DeadLetterRecord> deadLetters) {
    }
}
