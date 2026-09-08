package com.stream.processing.flink.config;

import com.stream.processing.common.SensorType;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * Every knob the job exposes, resolved once on the client before the graph is built.
 *
 * <p>Resolution order is job argument, then environment variable, then compiled-in default.
 * The argument wins because that is what an operator types when firefighting; the environment
 * variable exists because the container orchestrator is what supplies the broker address in a
 * deployed environment and re-templating a command line per environment is how drift starts.
 * The environment variable for a parameter is its name upper-cased with dots turned into
 * underscores, so {@code --kafka.bootstrap.servers} reads {@code KAFKA_BOOTSTRAP_SERVERS}.</p>
 *
 * <p>Serializable because the threshold policy it carries is captured by the operators and has
 * to survive the trip to the task managers.</p>
 */
public final class PipelineConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String PARAM_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    public static final String PARAM_GROUP_ID = "kafka.group.id";
    public static final String PARAM_WINDOW_SIZE_MINUTES = "window.size.minutes";
    public static final String PARAM_OUT_OF_ORDERNESS_SECONDS = "watermark.out.of.orderness.seconds";
    public static final String PARAM_IDLE_TIMEOUT_SECONDS = "watermark.idle.timeout.seconds";
    public static final String PARAM_CHECKPOINT_INTERVAL_MS = "checkpoint.interval.ms";
    public static final String PARAM_PARALLELISM = "parallelism";
    public static final String PARAM_SOURCE_START_OFFSET = "source.start.offset";

    public static final String PARAM_RAINFALL_WARNING = "threshold.rainfall.warning";
    public static final String PARAM_RAINFALL_SEVERE = "threshold.rainfall.severe";
    public static final String PARAM_RAINFALL_EXTREME = "threshold.rainfall.extreme";
    public static final String PARAM_RESERVOIR_WARNING = "threshold.reservoir.warning";
    public static final String PARAM_RESERVOIR_SEVERE = "threshold.reservoir.severe";
    public static final String PARAM_RESERVOIR_EXTREME = "threshold.reservoir.extreme";
    public static final String PARAM_RIVER_WARNING = "threshold.river.warning";
    public static final String PARAM_RIVER_SEVERE = "threshold.river.severe";
    public static final String PARAM_RIVER_EXTREME = "threshold.river.extreme";

    /** Metric name published on rainfall alerts, naming both the quantity and how it was reduced. */
    public static final String METRIC_RAINFALL = "RAINFALL_WINDOW_SUM";
    public static final String METRIC_RESERVOIR = "RESERVOIR_LEVEL_WINDOW_MAX";
    public static final String METRIC_RIVER = "RIVER_LEVEL_WINDOW_MAX";

    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String DEFAULT_GROUP_ID = "stream-flink-pipeline";
    public static final int DEFAULT_WINDOW_SIZE_MINUTES = 5;
    public static final long DEFAULT_OUT_OF_ORDERNESS_SECONDS = 30L;
    public static final long DEFAULT_IDLE_TIMEOUT_SECONDS = 60L;
    public static final long DEFAULT_CHECKPOINT_INTERVAL_MS = 30_000L;
    public static final int DEFAULT_PARALLELISM = 3;
    public static final String DEFAULT_SOURCE_START_OFFSET = "latest";

    public static final double DEFAULT_RAINFALL_WARNING = 15.0d;
    public static final double DEFAULT_RAINFALL_SEVERE = 30.0d;
    public static final double DEFAULT_RAINFALL_EXTREME = 50.0d;
    public static final double DEFAULT_RESERVOIR_WARNING = 85.0d;
    public static final double DEFAULT_RESERVOIR_SEVERE = 95.0d;
    public static final double DEFAULT_RESERVOIR_EXTREME = 100.0d;
    public static final double DEFAULT_RIVER_WARNING = 8.0d;
    public static final double DEFAULT_RIVER_SEVERE = 10.0d;
    public static final double DEFAULT_RIVER_EXTREME = 12.0d;

    private static final String OFFSET_EARLIEST = "earliest";
    private static final String OFFSET_LATEST = "latest";

    private final String kafkaBootstrapServers;
    private final String kafkaGroupId;
    private final int windowSizeMinutes;
    private final long outOfOrdernessSeconds;
    private final long idleTimeoutSeconds;
    private final long checkpointIntervalMs;
    private final int parallelism;
    private final String sourceStartOffset;
    private final ThresholdRuleSet thresholds;

    private PipelineConfig(Builder builder) {
        this.kafkaBootstrapServers = builder.kafkaBootstrapServers;
        this.kafkaGroupId = builder.kafkaGroupId;
        this.windowSizeMinutes = builder.windowSizeMinutes;
        this.outOfOrdernessSeconds = builder.outOfOrdernessSeconds;
        this.idleTimeoutSeconds = builder.idleTimeoutSeconds;
        this.checkpointIntervalMs = builder.checkpointIntervalMs;
        this.parallelism = builder.parallelism;
        this.sourceStartOffset = builder.sourceStartOffset;
        this.thresholds = builder.thresholds;
    }

    /** Resolves configuration from command-line arguments, falling back to the process environment. */
    public static PipelineConfig fromArgs(String... args) {
        return from(ParameterTool.fromArgs(args), System::getenv);
    }

    /**
     * Resolves configuration against an explicit environment lookup, which is what makes the
     * fallback chain testable without mutating the real process environment.
     */
    public static PipelineConfig from(ParameterTool params, UnaryOperator<String> environment) {
        Objects.requireNonNull(params, "params");
        Objects.requireNonNull(environment, "environment");
        Resolver resolver = new Resolver(params, environment);

        Map<SensorType, ThresholdRule> rules = new EnumMap<>(SensorType.class);
        rules.put(SensorType.RAINFALL, new ThresholdRule(
                METRIC_RAINFALL,
                resolver.asDouble(PARAM_RAINFALL_WARNING, DEFAULT_RAINFALL_WARNING),
                resolver.asDouble(PARAM_RAINFALL_SEVERE, DEFAULT_RAINFALL_SEVERE),
                resolver.asDouble(PARAM_RAINFALL_EXTREME, DEFAULT_RAINFALL_EXTREME)));
        rules.put(SensorType.RESERVOIR_LEVEL, new ThresholdRule(
                METRIC_RESERVOIR,
                resolver.asDouble(PARAM_RESERVOIR_WARNING, DEFAULT_RESERVOIR_WARNING),
                resolver.asDouble(PARAM_RESERVOIR_SEVERE, DEFAULT_RESERVOIR_SEVERE),
                resolver.asDouble(PARAM_RESERVOIR_EXTREME, DEFAULT_RESERVOIR_EXTREME)));
        rules.put(SensorType.RIVER_LEVEL, new ThresholdRule(
                METRIC_RIVER,
                resolver.asDouble(PARAM_RIVER_WARNING, DEFAULT_RIVER_WARNING),
                resolver.asDouble(PARAM_RIVER_SEVERE, DEFAULT_RIVER_SEVERE),
                resolver.asDouble(PARAM_RIVER_EXTREME, DEFAULT_RIVER_EXTREME)));

        Builder builder = new Builder();
        builder.kafkaBootstrapServers = resolver.asString(PARAM_BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_SERVERS);
        builder.kafkaGroupId = resolver.asString(PARAM_GROUP_ID, DEFAULT_GROUP_ID);
        builder.windowSizeMinutes = positiveInt(PARAM_WINDOW_SIZE_MINUTES,
                resolver.asInt(PARAM_WINDOW_SIZE_MINUTES, DEFAULT_WINDOW_SIZE_MINUTES));
        builder.outOfOrdernessSeconds = notNegative(PARAM_OUT_OF_ORDERNESS_SECONDS,
                resolver.asLong(PARAM_OUT_OF_ORDERNESS_SECONDS, DEFAULT_OUT_OF_ORDERNESS_SECONDS));
        builder.idleTimeoutSeconds = notNegative(PARAM_IDLE_TIMEOUT_SECONDS,
                resolver.asLong(PARAM_IDLE_TIMEOUT_SECONDS, DEFAULT_IDLE_TIMEOUT_SECONDS));
        builder.checkpointIntervalMs = positive(PARAM_CHECKPOINT_INTERVAL_MS,
                resolver.asLong(PARAM_CHECKPOINT_INTERVAL_MS, DEFAULT_CHECKPOINT_INTERVAL_MS));
        builder.parallelism = positiveInt(PARAM_PARALLELISM,
                resolver.asInt(PARAM_PARALLELISM, DEFAULT_PARALLELISM));
        builder.sourceStartOffset = normaliseOffset(
                resolver.asString(PARAM_SOURCE_START_OFFSET, DEFAULT_SOURCE_START_OFFSET));
        builder.thresholds = new ThresholdRuleSet(rules);
        return new PipelineConfig(builder);
    }

    private static int positiveInt(String name, int value) {
        return Math.toIntExact(positive(name, value));
    }

    private static long positive(String name, long value) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be greater than zero but was " + value);
        }
        return value;
    }

    private static long notNegative(String name, long value) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative but was " + value);
        }
        return value;
    }

    private static String normaliseOffset(String value) {
        String lower = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
        if (OFFSET_EARLIEST.equals(lower) || OFFSET_LATEST.equals(lower)) {
            return lower;
        }
        throw new IllegalArgumentException(PARAM_SOURCE_START_OFFSET
                + " must be 'earliest' or 'latest' but was '" + value + "'");
    }

    /** The environment variable consulted when a parameter is not supplied as an argument. */
    public static String environmentVariableFor(String parameterName) {
        return parameterName.toUpperCase(Locale.ROOT).replace('.', '_');
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getKafkaGroupId() {
        return kafkaGroupId;
    }

    public int getWindowSizeMinutes() {
        return windowSizeMinutes;
    }

    public Duration getWindowSize() {
        return Duration.ofMinutes(windowSizeMinutes);
    }

    public long getOutOfOrdernessSeconds() {
        return outOfOrdernessSeconds;
    }

    public Duration getOutOfOrderness() {
        return Duration.ofSeconds(outOfOrdernessSeconds);
    }

    public long getIdleTimeoutSeconds() {
        return idleTimeoutSeconds;
    }

    public Duration getIdleTimeout() {
        return Duration.ofSeconds(idleTimeoutSeconds);
    }

    public long getCheckpointIntervalMs() {
        return checkpointIntervalMs;
    }

    public int getParallelism() {
        return parallelism;
    }

    public String getSourceStartOffset() {
        return sourceStartOffset;
    }

    public boolean startsFromEarliest() {
        return OFFSET_EARLIEST.equals(sourceStartOffset);
    }

    public ThresholdRuleSet getThresholds() {
        return thresholds;
    }

    @Override
    public String toString() {
        return "PipelineConfig{bootstrapServers=" + kafkaBootstrapServers
                + ", groupId=" + kafkaGroupId
                + ", windowSizeMinutes=" + windowSizeMinutes
                + ", outOfOrdernessSeconds=" + outOfOrdernessSeconds
                + ", idleTimeoutSeconds=" + idleTimeoutSeconds
                + ", checkpointIntervalMs=" + checkpointIntervalMs
                + ", parallelism=" + parallelism
                + ", sourceStartOffset=" + sourceStartOffset
                + ", thresholds=" + thresholds
                + '}';
    }

    /** Mutable carrier used only while assembling an instance; never escapes this class. */
    private static final class Builder {
        private String kafkaBootstrapServers;
        private String kafkaGroupId;
        private int windowSizeMinutes;
        private long outOfOrdernessSeconds;
        private long idleTimeoutSeconds;
        private long checkpointIntervalMs;
        private int parallelism;
        private String sourceStartOffset;
        private ThresholdRuleSet thresholds;
    }

    /** Argument, then environment, then default — applied identically to every parameter. */
    private static final class Resolver {

        private final ParameterTool params;
        private final UnaryOperator<String> environment;

        private Resolver(ParameterTool params, UnaryOperator<String> environment) {
            this.params = params;
            this.environment = environment;
        }

        private String raw(String name) {
            if (params.has(name)) {
                String fromArgs = params.get(name);
                if (fromArgs != null && !fromArgs.isBlank()) {
                    return fromArgs.trim();
                }
            }
            String fromEnv = environment.apply(environmentVariableFor(name));
            return fromEnv == null || fromEnv.isBlank() ? null : fromEnv.trim();
        }

        private String asString(String name, String fallback) {
            String value = raw(name);
            return value == null ? fallback : value;
        }

        private int asInt(String name, int fallback) {
            String value = raw(name);
            if (value == null) {
                return fallback;
            }
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(name + " must be an integer but was '" + value + "'", e);
            }
        }

        private long asLong(String name, long fallback) {
            String value = raw(name);
            if (value == null) {
                return fallback;
            }
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(name + " must be a whole number but was '" + value + "'", e);
            }
        }

        private double asDouble(String name, double fallback) {
            String value = raw(name);
            if (value == null) {
                return fallback;
            }
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(name + " must be a number but was '" + value + "'", e);
            }
        }
    }
}
