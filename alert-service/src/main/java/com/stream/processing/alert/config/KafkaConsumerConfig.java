package com.stream.processing.alert.config;

import com.stream.processing.alert.service.AlertMetrics;
import com.stream.processing.common.Topics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.BackOff;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumption for both feeds.
 *
 * <p>Three decisions here shape the service's delivery guarantees, and all three are deliberate:</p>
 * <ul>
 *   <li><strong>Byte arrays, not a JSON deserializer.</strong> Payloads are decoded inside the
 *       listener with the shared {@link com.stream.processing.common.JsonCodec}. That keeps the wire
 *       format owned by one class, and - more importantly - it puts a malformed record's failure
 *       inside the listener, where it can be reported as a
 *       {@link org.springframework.kafka.listener.BatchListenerFailedException} naming the exact
 *       offset. A deserializer failure happens before the listener runs and can only be handled
 *       as "something in this batch is bad".</li>
 *   <li><strong>Manual immediate acknowledgement, auto-commit off.</strong> The offset advances
 *       only after the transaction has committed, so a crash between persisting and acknowledging
 *       replays the batch - which the idempotent upsert absorbs. Auto-commit would do the
 *       opposite: acknowledge on a timer and lose alerts on a crash.</li>
 *   <li><strong>Concurrency matched to the partition count.</strong> Three listener threads for
 *       three partitions: fewer would leave a partition sharing a thread and lagging, more would
 *       leave threads permanently idle since a partition is never consumed by two members of the
 *       same group.</li>
 * </ul>
 */
@Configuration(proxyBeanMethods = false)
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    private final KafkaProperties kafkaProperties;
    private final AlertServiceProperties properties;

    public KafkaConsumerConfig(KafkaProperties kafkaProperties, AlertServiceProperties properties) {
        this.kafkaProperties = kafkaProperties;
        this.properties = properties;
    }

    /**
     * Shared consumer factory. The group id is intentionally absent: each listener declares its
     * own, so the alert feed and the aggregate feed track offsets independently and switching the
     * aggregate feed off cannot strand the alert feed's committed position.
     */
    @Bean
    public ConsumerFactory<String, byte[]> streamConsumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        // Never skip past unread history. A fresh deployment - or a group whose offsets have been
        // aged out - must reprocess what is retained rather than silently discarding alerts that
        // were produced while it was down; the upsert makes reprocessing free of side effects.
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.consumer().maxPollRecords());

        return new DefaultKafkaConsumerFactory<>(config);
    }

    /** Container factory for {@link Topics#GENERATED_ALERTS}. */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]> alertListenerContainerFactory(
            ConsumerFactory<String, byte[]> streamConsumerFactory,
            DefaultErrorHandler streamErrorHandler) {
        return batchFactory(streamConsumerFactory, streamErrorHandler);
    }

    /**
     * Container factory for {@link Topics#AGGREGATED_TELEMETRY}.
     *
     * <p>A separate bean with identical settings rather than a shared one, so the higher-volume
     * observability feed can be throttled independently of the alerting path without touching the
     * feed the platform actually exists to serve.</p>
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]> aggregateListenerContainerFactory(
            ConsumerFactory<String, byte[]> streamConsumerFactory,
            DefaultErrorHandler streamErrorHandler) {
        return batchFactory(streamConsumerFactory, streamErrorHandler);
    }

    private ConcurrentKafkaListenerContainerFactory<String, byte[]> batchFactory(
            ConsumerFactory<String, byte[]> consumerFactory, DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(properties.consumer().concurrency());
        factory.setCommonErrorHandler(errorHandler);

        ContainerProperties containerProperties = factory.getContainerProperties();
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        containerProperties.setPollTimeout(properties.consumer().pollTimeout().toMillis());
        // Emits Micrometer timers per listener, which is how the Grafana dashboard shows consumer
        // throughput next to the pipeline-latency histogram.
        containerProperties.setObservationEnabled(false);
        containerProperties.setMicrometerEnabled(true);

        return factory;
    }

    /**
     * Producer used only by the dead-letter recoverer. Byte-array valued because a poison record
     * is republished verbatim - re-serialising a payload that failed to parse is not possible, and
     * the original bytes are what an operator needs in order to work out what the producer sent.
     */
    @Bean
    public ProducerFactory<String, byte[]> deadLetterProducerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        // acks=all: the whole point of the DLQ is that the record is not lost, so a publish that
        // only reached the leader would defeat it.
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, byte[]> deadLetterKafkaTemplate(
            ProducerFactory<String, byte[]> deadLetterProducerFactory) {
        return new KafkaTemplate<>(deadLetterProducerFactory);
    }

    /**
     * Retries a failing batch with exponential backoff, then routes the offending record to
     * {@link Topics#DEAD_LETTER} and moves on.
     *
     * <p>Both halves matter. The backoff covers the transient failure - a database failover, a
     * saturated connection pool - where the same batch will succeed shortly. The recoverer covers
     * the permanent one: without it, a single record the producer serialised wrongly would be
     * retried forever and every alert behind it on that partition would never be stored. The
     * partition must keep moving even when one record cannot.</p>
     */
    @Bean
    public DefaultErrorHandler streamErrorHandler(KafkaTemplate<String, byte[]> deadLetterKafkaTemplate,
                                                 AlertMetrics metrics) {
        DeadLetterPublishingRecoverer publisher = new DeadLetterPublishingRecoverer(
                deadLetterKafkaTemplate,
                // Partition -1 lets the broker choose by key rather than assuming the DLQ has the
                // same partition count as the source topic.
                (record, exception) -> new TopicPartition(Topics.DEAD_LETTER, -1));

        return new DefaultErrorHandler((record, exception) -> {
            log.error("Dead-lettering record from {}-{} at offset {}: {}",
                    record.topic(), record.partition(), record.offset(), exception.getMessage());
            metrics.deadLettered();
            publisher.accept(record, exception);
        }, backOff());
    }

    private BackOff backOff() {
        AlertServiceProperties.Retry retry = properties.retry();
        ExponentialBackOffWithMaxRetries backOff =
                new ExponentialBackOffWithMaxRetries(retry.maxAttempts());
        backOff.setInitialInterval(retry.initialInterval().toMillis());
        backOff.setMultiplier(retry.multiplier());
        backOff.setMaxInterval(retry.maxInterval().toMillis());
        return backOff;
    }
}
