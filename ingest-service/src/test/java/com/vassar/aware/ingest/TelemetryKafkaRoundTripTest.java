package com.vassar.aware.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.vassar.aware.common.JsonCodec;
import com.vassar.aware.common.SensorReading;
import com.vassar.aware.common.SensorType;
import com.vassar.aware.common.Topics;
import com.vassar.aware.ingest.publish.TelemetryPublisher;
import com.vassar.aware.ingest.scrape.TelemetryScraper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The wire-contract test: a reading published through the real producer configuration is read
 * back off the broker and must deserialise, byte for byte, into the same event.
 *
 * <p>Every other test in this module mocks the template, so this is the only place that proves
 * the combination of {@code JsonCodec}, the {@code JsonSerializer} and the topic layout actually
 * produces what the Flink stage will subscribe to. It also asserts the raw JSON shape rather than
 * only the round-tripped object, because Flink deserialises the bytes with its own mapper: an
 * instant silently written as an epoch decimal would round-trip here and still break downstream.</p>
 *
 * <p>The scheduled simulator is off - the test profile disables it - so nothing competes for the
 * topic, and every wait is bounded so a broker that never comes up fails the build instead of
 * hanging it.</p>
 */
@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 3, count = 1)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class TelemetryKafkaRoundTripTest {

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(20);
    private static final Instant EVENT_TIME = Instant.parse("2026-08-11T06:00:00Z");

    @Autowired
    private TelemetryPublisher publisher;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private ObjectProvider<TelemetryScraper> scraperProvider;

    private Consumer<String, String> consumer;

    /**
     * Partitions are assigned explicitly and the consumer is parked at the end of the log before
     * each test. Subscribing to a group instead would leave the test at the mercy of rebalance
     * timing, and reading from the start would let one test see the records another one wrote.
     */
    @BeforeEach
    void seekToEndOfTopics() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ingest-round-trip");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // Deserialised as text on purpose: the assertions are about the bytes on the topic.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 20_000);
        consumer = new KafkaConsumer<>(props);

        List<TopicPartition> partitions = new ArrayList<>();
        for (String topic : List.of(Topics.RAW_TELEMETRY, Topics.DEAD_LETTER)) {
            for (PartitionInfo info : consumer.partitionsFor(topic)) {
                partitions.add(new TopicPartition(info.topic(), info.partition()));
            }
        }
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);
        // seekToEnd is lazy; asking for the position resolves it before anything is published.
        partitions.forEach(consumer::position);
    }

    @AfterEach
    void unsubscribe() {
        if (consumer != null) {
            consumer.close(Duration.ofSeconds(5));
        }
    }

    private static SensorReading reading(String stationId, double value) {
        SensorReading reading = new SensorReading();
        reading.setReadingId("11111111-2222-3333-4444-555555555555");
        reading.setStationId(stationId);
        reading.setStationName("Vijayawada Rain Gauge");
        reading.setDistrictId("AP-NTR");
        reading.setSensorType(SensorType.RAINFALL);
        reading.setUnit(SensorType.RAINFALL.getUnit());
        reading.setValue(value);
        reading.setLatitude(16.51d);
        reading.setLongitude(80.62d);
        reading.setEventTime(EVENT_TIME);
        reading.setSource("AWARE_SIMULATOR");
        return reading;
    }

    /** Polls until {@code expected} records have arrived or the timeout expires. */
    private List<ConsumerRecord<String, String>> drain(int expected) {
        List<ConsumerRecord<String, String>> collected = new ArrayList<>();
        long deadline = System.nanoTime() + POLL_TIMEOUT.toNanos();
        while (collected.size() < expected && System.nanoTime() < deadline) {
            ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
            polled.forEach(collected::add);
        }
        return collected;
    }

    @Test
    @DisplayName("the topics are created with the partition count the downstream job assumes")
    void topicsAreCreatedWithTheAgreedGeometry() {
        assertThat(consumer.partitionsFor(Topics.RAW_TELEMETRY)).hasSize(3);
        assertThat(consumer.partitionsFor(Topics.AGGREGATED_TELEMETRY)).hasSize(3);
        assertThat(consumer.partitionsFor(Topics.GENERATED_ALERTS)).hasSize(3);
        assertThat(consumer.partitionsFor(Topics.DEAD_LETTER)).hasSize(3);
    }

    @Test
    @DisplayName("the scheduled simulator is off, so nothing else writes to the topic")
    void scheduledSimulatorIsDisabledInThisSlice() {
        assertThat(scraperProvider.getIfAvailable()).isNull();
    }

    @Test
    @DisplayName("a published reading round-trips through Kafka unchanged")
    void readingRoundTripsThroughKafka() throws Exception {
        SensorReading sent = reading("STN-0001", 7.25d);

        publisher.publish(sent).get(30, TimeUnit.SECONDS);

        List<ConsumerRecord<String, String>> records = drain(1);
        assertThat(records).hasSize(1);
        ConsumerRecord<String, String> record = records.get(0);

        assertThat(record.topic()).isEqualTo(Topics.RAW_TELEMETRY);
        assertThat(record.key()).as("keyed by station so the downstream keyed windows stay ordered")
                .isEqualTo("STN-0001");

        SensorReading received = JsonCodec.fromJson(record.value(), SensorReading.class);
        assertThat(received).isEqualTo(sent);
        assertThat(received.isValid()).isTrue();
        assertThat(received.getEventTime()).isEqualTo(EVENT_TIME);
        assertThat(received.getIngestedAt()).isNotNull();

        JsonNode payload = JsonCodec.mapper().readTree(record.value());
        assertThat(payload.get("eventTime").asText())
                .as("ISO-8601 text, not an epoch decimal, or the Flink stage cannot read it")
                .isEqualTo("2026-08-11T06:00:00Z");
        assertThat(payload.get("sensorType").asText()).isEqualTo("RAINFALL");
        assertThat(payload.get("unit").asText()).isEqualTo("MM");
        assertThat(payload.get("value").asDouble()).isEqualTo(7.25d);
        assertThat(payload.has("@class")).as("no Jackson type header on the wire").isFalse();
    }

    @Test
    @DisplayName("an invalid reading lands on the dead-letter topic with its reason")
    void invalidReadingIsRoutedToTheDeadLetterTopic() throws Exception {
        SensorReading broken = reading("STN-0002", 5.0d);
        broken.setEventTime(null);

        publisher.publish(broken).get(30, TimeUnit.SECONDS);

        List<ConsumerRecord<String, String>> records = drain(1);
        assertThat(records).hasSize(1);
        ConsumerRecord<String, String> record = records.get(0);

        assertThat(record.topic()).isEqualTo(Topics.DEAD_LETTER);
        assertThat(record.key()).isEqualTo("STN-0002");
        assertThat(new String(record.headers().lastHeader(TelemetryPublisher.DLQ_REASON_HEADER).value(),
                StandardCharsets.UTF_8)).isEqualTo("missing eventTime");
        assertThat(JsonCodec.fromJson(record.value(), SensorReading.class).getStationId())
                .isEqualTo("STN-0002");
    }

    @Test
    @DisplayName("a station's readings all land on one partition, preserving their order")
    void aStationsReadingsShareOnePartition() throws Exception {
        for (int i = 0; i < 5; i++) {
            publisher.publish(reading("STN-0003", 1.0d + i)).get(30, TimeUnit.SECONDS);
        }

        List<ConsumerRecord<String, String>> records = drain(5);

        assertThat(records).hasSize(5);
        assertThat(records).extracting(ConsumerRecord::partition).containsOnly(records.get(0).partition());
        assertThat(records).extracting(consumed ->
                        JsonCodec.fromJson(consumed.value(), SensorReading.class).getValue())
                .containsExactly(1.0d, 2.0d, 3.0d, 4.0d, 5.0d);
    }
}
