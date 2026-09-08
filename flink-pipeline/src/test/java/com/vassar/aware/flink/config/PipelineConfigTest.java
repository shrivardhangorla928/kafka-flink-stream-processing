package com.vassar.aware.flink.config;

import com.vassar.aware.common.SensorType;
import com.vassar.aware.common.Severity;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Pins the argument-then-environment-then-default resolution chain. */
@Timeout(30)
class PipelineConfigTest {

    private static final UnaryOperator<String> NO_ENVIRONMENT = name -> null;

    private static PipelineConfig configure(Map<String, String> args, Map<String, String> environment) {
        return PipelineConfig.from(ParameterTool.fromMap(args), environment::get);
    }

    @Test
    void fallsBackToDefaultsWhenNothingIsSupplied() {
        PipelineConfig config = PipelineConfig.from(ParameterTool.fromMap(Map.of()), NO_ENVIRONMENT);

        assertThat(config.getKafkaBootstrapServers()).isEqualTo("localhost:9092");
        assertThat(config.getKafkaGroupId()).isEqualTo("aware-flink-pipeline");
        assertThat(config.getWindowSizeMinutes()).isEqualTo(5);
        assertThat(config.getWindowSize()).isEqualTo(Duration.ofMinutes(5));
        assertThat(config.getOutOfOrdernessSeconds()).isEqualTo(30L);
        assertThat(config.getIdleTimeoutSeconds()).isEqualTo(60L);
        assertThat(config.getCheckpointIntervalMs()).isEqualTo(30_000L);
        assertThat(config.getParallelism()).isEqualTo(3);
        assertThat(config.getSourceStartOffset()).isEqualTo("latest");
        assertThat(config.startsFromEarliest()).isFalse();
    }

    @Test
    void carriesTheDocumentedDefaultThresholdBands() {
        ThresholdRuleSet thresholds =
                PipelineConfig.from(ParameterTool.fromMap(Map.of()), NO_ENVIRONMENT).getThresholds();

        ThresholdRule rainfall = thresholds.ruleFor(SensorType.RAINFALL);
        assertThat(rainfall.getMetric()).isEqualTo("RAINFALL_WINDOW_SUM");
        assertThat(rainfall.getWarning()).isEqualTo(15.0d);
        assertThat(rainfall.getSevere()).isEqualTo(30.0d);
        assertThat(rainfall.getExtreme()).isEqualTo(50.0d);

        ThresholdRule reservoir = thresholds.ruleFor(SensorType.RESERVOIR_LEVEL);
        assertThat(reservoir.getMetric()).isEqualTo("RESERVOIR_LEVEL_WINDOW_MAX");
        assertThat(reservoir.getWarning()).isEqualTo(85.0d);
        assertThat(reservoir.getSevere()).isEqualTo(95.0d);
        assertThat(reservoir.getExtreme()).isEqualTo(100.0d);

        ThresholdRule river = thresholds.ruleFor(SensorType.RIVER_LEVEL);
        assertThat(river.getMetric()).isEqualTo("RIVER_LEVEL_WINDOW_MAX");
        assertThat(river.getWarning()).isEqualTo(8.0d);
        assertThat(river.getSevere()).isEqualTo(10.0d);
        assertThat(river.getExtreme()).isEqualTo(12.0d);
    }

    @Test
    void readsEveryParameterFromArguments() {
        PipelineConfig config = PipelineConfig.fromArgs(
                "--kafka.bootstrap.servers", "broker-1:9092,broker-2:9092",
                "--kafka.group.id", "aware-flink-canary",
                "--window.size.minutes", "15",
                "--watermark.out.of.orderness.seconds", "90",
                "--watermark.idle.timeout.seconds", "120",
                "--checkpoint.interval.ms", "60000",
                "--parallelism", "6",
                "--source.start.offset", "earliest",
                "--threshold.rainfall.warning", "10",
                "--threshold.rainfall.severe", "20",
                "--threshold.rainfall.extreme", "40",
                "--threshold.reservoir.warning", "70",
                "--threshold.reservoir.severe", "80",
                "--threshold.reservoir.extreme", "90",
                "--threshold.river.warning", "4",
                "--threshold.river.severe", "6",
                "--threshold.river.extreme", "9");

        assertThat(config.getKafkaBootstrapServers()).isEqualTo("broker-1:9092,broker-2:9092");
        assertThat(config.getKafkaGroupId()).isEqualTo("aware-flink-canary");
        assertThat(config.getWindowSizeMinutes()).isEqualTo(15);
        assertThat(config.getOutOfOrderness()).isEqualTo(Duration.ofSeconds(90));
        assertThat(config.getIdleTimeout()).isEqualTo(Duration.ofSeconds(120));
        assertThat(config.getCheckpointIntervalMs()).isEqualTo(60_000L);
        assertThat(config.getParallelism()).isEqualTo(6);
        assertThat(config.startsFromEarliest()).isTrue();
        assertThat(config.getThresholds().ruleFor(SensorType.RIVER_LEVEL).highestBreachedBand(6.5d))
                .isEqualTo(Severity.SEVERE);
        assertThat(config.getThresholds().ruleFor(SensorType.RAINFALL).getExtreme()).isEqualTo(40.0d);
        assertThat(config.getThresholds().ruleFor(SensorType.RESERVOIR_LEVEL).getWarning()).isEqualTo(70.0d);
    }

    @Test
    void fallsBackToTheEnvironmentWhenAnArgumentIsAbsent() {
        PipelineConfig config = configure(Map.of(), Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka.svc.cluster.local:9092",
                "THRESHOLD_RIVER_WARNING", "5.5",
                "PARALLELISM", "9"));

        assertThat(config.getKafkaBootstrapServers()).isEqualTo("kafka.svc.cluster.local:9092");
        assertThat(config.getParallelism()).isEqualTo(9);
        assertThat(config.getThresholds().ruleFor(SensorType.RIVER_LEVEL).getWarning()).isEqualTo(5.5d);
        // Anything the environment did not set still falls through to the default.
        assertThat(config.getKafkaGroupId()).isEqualTo("aware-flink-pipeline");
    }

    @Test
    void argumentsWinOverTheEnvironment() {
        PipelineConfig config = configure(
                Map.of(PipelineConfig.PARAM_BOOTSTRAP_SERVERS, "from-args:9092"),
                Map.of("KAFKA_BOOTSTRAP_SERVERS", "from-env:9092"));

        assertThat(config.getKafkaBootstrapServers()).isEqualTo("from-args:9092");
    }

    @Test
    void derivesEnvironmentVariableNamesFromParameterNames() {
        assertThat(PipelineConfig.environmentVariableFor(PipelineConfig.PARAM_BOOTSTRAP_SERVERS))
                .isEqualTo("KAFKA_BOOTSTRAP_SERVERS");
        assertThat(PipelineConfig.environmentVariableFor(PipelineConfig.PARAM_RAINFALL_EXTREME))
                .isEqualTo("THRESHOLD_RAINFALL_EXTREME");
        assertThat(PipelineConfig.environmentVariableFor(PipelineConfig.PARAM_SOURCE_START_OFFSET))
                .isEqualTo("SOURCE_START_OFFSET");
    }

    @Test
    void treatsBlankValuesAsAbsentSoAnEmptyTemplateVariableDoesNotWinSilently() {
        PipelineConfig config = configure(
                Map.of(PipelineConfig.PARAM_BOOTSTRAP_SERVERS, "   "),
                Map.of("KAFKA_BOOTSTRAP_SERVERS", "  "));

        assertThat(config.getKafkaBootstrapServers()).isEqualTo("localhost:9092");
    }

    @Test
    void trimsSurroundingWhitespace() {
        PipelineConfig config = configure(Map.of(), Map.of(
                "KAFKA_GROUP_ID", "  aware-flink-pipeline-b  ",
                "SOURCE_START_OFFSET", " EARLIEST "));

        assertThat(config.getKafkaGroupId()).isEqualTo("aware-flink-pipeline-b");
        assertThat(config.startsFromEarliest()).isTrue();
    }

    @Test
    void rejectsAnUnknownStartOffset() {
        assertThatThrownBy(() -> configure(
                Map.of(PipelineConfig.PARAM_SOURCE_START_OFFSET, "beginning"), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source.start.offset");
    }

    @Test
    void rejectsNonNumericValuesInsteadOfSilentlyDefaulting() {
        assertThatThrownBy(() -> configure(Map.of(PipelineConfig.PARAM_PARALLELISM, "three"), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("parallelism");
        assertThatThrownBy(() -> configure(
                Map.of(PipelineConfig.PARAM_CHECKPOINT_INTERVAL_MS, "often"), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("checkpoint.interval.ms");
        assertThatThrownBy(() -> configure(Map.of(PipelineConfig.PARAM_RAINFALL_SEVERE, "lots"), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("threshold.rainfall.severe");
    }

    @Test
    void rejectsValuesThatWouldProduceAnUnrunnableJob() {
        assertThatThrownBy(() -> configure(Map.of(PipelineConfig.PARAM_WINDOW_SIZE_MINUTES, "0"), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("greater than zero");
        assertThatThrownBy(() -> configure(Map.of(PipelineConfig.PARAM_PARALLELISM, "-1"), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("greater than zero");
        assertThatThrownBy(() -> configure(
                Map.of(PipelineConfig.PARAM_OUT_OF_ORDERNESS_SECONDS, "-5"), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be negative");
    }

    @Test
    void allowsZeroLatenessAndZeroIdlenessAsAWayToDisableThem() {
        PipelineConfig config = configure(Map.of(
                PipelineConfig.PARAM_OUT_OF_ORDERNESS_SECONDS, "0",
                PipelineConfig.PARAM_IDLE_TIMEOUT_SECONDS, "0"), Map.of());

        assertThat(config.getOutOfOrdernessSeconds()).isZero();
        assertThat(config.getIdleTimeoutSeconds()).isZero();
    }

    @Test
    void rejectsThresholdBandsThatAreNotOrdered() {
        // A severe band below the warning band would make the highest-band-wins scan meaningless.
        assertThatThrownBy(() -> configure(Map.of(
                PipelineConfig.PARAM_RAINFALL_WARNING, "40",
                PipelineConfig.PARAM_RAINFALL_SEVERE, "20"), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-decreasing");
    }

    @Test
    void describesItselfWithoutLeakingNulls() {
        assertThat(PipelineConfig.from(ParameterTool.fromMap(Map.of()), NO_ENVIRONMENT).toString())
                .contains("bootstrapServers=localhost:9092")
                .contains("windowSizeMinutes=5")
                .contains("RAINFALL_WINDOW_SUM");
    }
}
