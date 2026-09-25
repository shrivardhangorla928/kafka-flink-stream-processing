package com.stream.processing.flink.config;

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
        assertThat(config.getKafkaGroupId()).isEqualTo("stream-flink-pipeline");
        assertThat(config.getOutOfOrdernessSeconds()).isEqualTo(30L);
        assertThat(config.getIdleTimeoutSeconds()).isEqualTo(60L);
        assertThat(config.getCheckpointIntervalMs()).isEqualTo(30_000L);
        assertThat(config.getParallelism()).isEqualTo(3);
        assertThat(config.getSourceStartOffset()).isEqualTo("latest");
        assertThat(config.startsFromEarliest()).isFalse();
    }

    @Test
    void readsEveryParameterFromArguments() {
        PipelineConfig config = PipelineConfig.fromArgs(
                "--kafka.bootstrap.servers", "broker-1:9092,broker-2:9092",
                "--kafka.group.id", "stream-flink-canary",
                "--watermark.out.of.orderness.seconds", "90",
                "--watermark.idle.timeout.seconds", "120",
                "--checkpoint.interval.ms", "60000",
                "--parallelism", "6",
                "--source.start.offset", "earliest");

        assertThat(config.getKafkaBootstrapServers()).isEqualTo("broker-1:9092,broker-2:9092");
        assertThat(config.getKafkaGroupId()).isEqualTo("stream-flink-canary");
        assertThat(config.getOutOfOrderness()).isEqualTo(Duration.ofSeconds(90));
        assertThat(config.getIdleTimeout()).isEqualTo(Duration.ofSeconds(120));
        assertThat(config.getCheckpointIntervalMs()).isEqualTo(60_000L);
        assertThat(config.getParallelism()).isEqualTo(6);
        assertThat(config.startsFromEarliest()).isTrue();
    }

    @Test
    void fallsBackToTheEnvironmentWhenAnArgumentIsAbsent() {
        PipelineConfig config = configure(Map.of(), Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka.svc.cluster.local:9092",
                "PARALLELISM", "9"));

        assertThat(config.getKafkaBootstrapServers()).isEqualTo("kafka.svc.cluster.local:9092");
        assertThat(config.getParallelism()).isEqualTo(9);
        // Anything the environment did not set still falls through to the default.
        assertThat(config.getKafkaGroupId()).isEqualTo("stream-flink-pipeline");
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
                "KAFKA_GROUP_ID", "  stream-flink-pipeline-b  ",
                "SOURCE_START_OFFSET", " EARLIEST "));

        assertThat(config.getKafkaGroupId()).isEqualTo("stream-flink-pipeline-b");
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
    }

    @Test
    void rejectsValuesThatWouldProduceAnUnrunnableJob() {
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
    void describesItselfWithoutLeakingNulls() {
        assertThat(PipelineConfig.from(ParameterTool.fromMap(Map.of()), NO_ENVIRONMENT).toString())
                .contains("bootstrapServers=localhost:9092")
                .contains("parallelism=3");
    }
}
