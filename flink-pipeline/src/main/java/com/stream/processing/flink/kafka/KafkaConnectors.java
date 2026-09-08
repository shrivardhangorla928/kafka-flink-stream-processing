package com.stream.processing.flink.kafka;

import com.stream.processing.common.Alert;
import com.stream.processing.common.StationWindowAggregate;
import com.stream.processing.common.Topics;
import com.stream.processing.flink.config.PipelineConfig;
import com.stream.processing.flink.ingest.DeadLetterRecord;
import com.stream.processing.flink.serde.JsonSerializationSchema;
import com.stream.processing.flink.serde.KeyExtractor;
import com.stream.processing.flink.serde.RawPayloadDeserializationSchema;
import com.stream.processing.flink.serde.StringKeySerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

/**
 * Builds the Kafka source and the three sinks, keeping connector wiring out of the topology.
 *
 * <p>Every sink runs at {@link DeliveryGuarantee#AT_LEAST_ONCE}. Exactly-once would need
 * transactional producers, and their transaction timeout has to be tuned to exceed the longest
 * plausible gap between checkpoints — get it wrong and the job dies after a restore with
 * {@code ProducerFencedException} or, worse, silently loses committed data. The alert stream does
 * not need that ceremony because {@code AlertIds.deterministicId} makes a replayed window produce
 * a byte-identical id, so the alert service's upsert absorbs the duplicate. The same reasoning
 * covers the aggregate topic, whose records are keyed by station and window bounds.</p>
 */
public final class KafkaConnectors {

    /** Consumer offsets are committed on checkpoint so lag is visible to standard Kafka tooling. */
    private static final String COMMIT_OFFSETS_ON_CHECKPOINT = "commit.offsets.on.checkpoint";

    private KafkaConnectors() {
        // static factory holder
    }

    /** Raw telemetry, read as bytes so malformed payloads can be dead-lettered rather than fatal. */
    public static KafkaSource<byte[]> rawTelemetrySource(PipelineConfig config) {
        return KafkaSource.<byte[]>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setGroupId(config.getKafkaGroupId())
                .setTopics(Topics.RAW_TELEMETRY)
                .setStartingOffsets(startingOffsets(config))
                .setValueOnlyDeserializer(new RawPayloadDeserializationSchema())
                .setProperty(COMMIT_OFFSETS_ON_CHECKPOINT, "true")
                .build();
    }

    /**
     * Where the source begins on a <em>fresh</em> start. A job resumed from a checkpoint or
     * savepoint restores its offsets from Flink state and ignores this entirely, which is why
     * defaulting to {@code latest} is safe: it only affects the very first submission, where
     * replaying weeks of historical telemetry would be the surprise, not the safe choice.
     * {@code earliest} exists for replaying a topic during evaluation.
     */
    private static OffsetsInitializer startingOffsets(PipelineConfig config) {
        return config.startsFromEarliest() ? OffsetsInitializer.earliest() : OffsetsInitializer.latest();
    }

    public static KafkaSink<StationWindowAggregate> aggregateSink(PipelineConfig config) {
        return jsonSink(config, Topics.AGGREGATED_TELEMETRY, StationWindowAggregate::getStationId);
    }

    public static KafkaSink<Alert> alertSink(PipelineConfig config) {
        return jsonSink(config, Topics.GENERATED_ALERTS, Alert::getStationId);
    }

    public static KafkaSink<DeadLetterRecord> deadLetterSink(PipelineConfig config) {
        // No natural key: the station could not be determined, which is why it is a dead letter.
        return jsonSink(config, Topics.DEAD_LETTER, record -> null);
    }

    private static <T> KafkaSink<T> jsonSink(PipelineConfig config, String topic, KeyExtractor<T> key) {
        KafkaRecordSerializationSchema<T> serializer = KafkaRecordSerializationSchema.<T>builder()
                .setTopic(topic)
                .setKeySerializationSchema(new StringKeySerializationSchema<>(key))
                .setValueSerializationSchema(new JsonSerializationSchema<T>())
                .build();
        return KafkaSink.<T>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setRecordSerializer(serializer)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
