package com.stream.processing.ingest.config;

import com.stream.processing.common.JsonCodec;
import com.stream.processing.common.SensorReading;
import com.stream.processing.common.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

/**
 * Producer wiring for the raw-telemetry stream.
 *
 * <p>The settings are chosen for a durable, ordered, high-throughput scrape rather than for
 * lowest possible latency: {@code acks=all} with idempotence guarantees no silent loss and no
 * duplicates on retry, which is what lets the pipeline claim effectively-once ingest; a 20 ms
 * linger with lz4 turns a burst of 50 per-station sends into a couple of compressed batches;
 * and an unbounded retry count bounded by a 120 s delivery timeout means a short broker
 * outage is ridden out inside the producer instead of surfacing as data loss.</p>
 *
 * <p>Idempotence keeps ordering per partition even with five in-flight requests, so the
 * concurrency does not have to be dropped to one.</p>
 */
@Configuration(proxyBeanMethods = false)
public class KafkaProducerConfig {

    private final KafkaProperties kafkaProperties;
    private final KafkaTopicProperties topicProperties;

    public KafkaProducerConfig(KafkaProperties kafkaProperties, KafkaTopicProperties topicProperties) {
        this.kafkaProperties = kafkaProperties;
        this.topicProperties = topicProperties;
    }

    @Bean
    public ProducerFactory<String, SensorReading> sensorReadingProducerFactory() {
        Map<String, Object> props = kafkaProperties.buildProducerProperties(null);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.TRUE);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32_768);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);

        // The serializers are supplied as instances so the shared JsonCodec configuration is used
        // verbatim; passing the classes instead would have Kafka build a default mapper.
        JsonSerializer<SensorReading> valueSerializer = new JsonSerializer<>(JsonCodec.create());
        valueSerializer.setAddTypeInfo(false);

        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), valueSerializer);
    }

    @Bean
    public KafkaTemplate<String, SensorReading> sensorReadingKafkaTemplate(
            ProducerFactory<String, SensorReading> sensorReadingProducerFactory) {
        KafkaTemplate<String, SensorReading> template = new KafkaTemplate<>(sensorReadingProducerFactory);
        template.setDefaultTopic(Topics.RAW_TELEMETRY);
        template.setObservationEnabled(false);
        return template;
    }

    @Bean
    public NewTopic rawTelemetryTopic() {
        return topic(Topics.RAW_TELEMETRY);
    }

    /**
     * Declared here even though this service never writes to it: the ingest service is the first
     * component to start in the deployment, so it is the natural place to guarantee every topic
     * exists with the agreed partition count before Flink subscribes.
     */
    @Bean
    public NewTopic aggregatedTelemetryTopic() {
        return topic(Topics.AGGREGATED_TELEMETRY);
    }

    @Bean
    public NewTopic generatedAlertsTopic() {
        return topic(Topics.GENERATED_ALERTS);
    }

    @Bean
    public NewTopic deadLetterTopic() {
        return topic(Topics.DEAD_LETTER);
    }

    private NewTopic topic(String name) {
        return TopicBuilder.name(name)
                .partitions(topicProperties.getPartitions())
                .replicas(topicProperties.getReplicationFactor())
                .build();
    }
}
