package com.vassar.aware.alert.kafka;

import com.vassar.aware.alert.AbstractIntegrationTest;
import com.vassar.aware.alert.AlertFixtures;
import com.vassar.aware.alert.domain.AlertEntity;
import com.vassar.aware.common.Alert;
import com.vassar.aware.common.JsonCodec;
import com.vassar.aware.common.SensorType;
import com.vassar.aware.common.Severity;
import com.vassar.aware.common.StationWindowAggregate;
import com.vassar.aware.common.Topics;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves the wire contract end to end: an {@link Alert} serialised exactly as the Flink job
 * serialises it, published to {@code alerts.generated.v1}, ends up as a row in the store.
 *
 * <p>This is the one test that would catch a divergence between the producers' JSON and this
 * service's reader - a renamed field, an instant written as an epoch decimal, an enum serialised
 * as an ordinal. Every other test in the module builds its objects in-process and so cannot see
 * such a break. The payload is produced with {@link JsonCodec} precisely because that is the
 * class the upstream modules use; hand-writing the JSON here would test the test.</p>
 *
 * <p>Bounded twice over: a JUnit {@code @Timeout} on each method and an Awaitility deadline on
 * each wait, so a broken listener fails the build in seconds instead of hanging CI.</p>
 */
class AlertWireContractIntegrationTest extends AbstractIntegrationTest {

    /** Generous enough for a cold consumer group to be assigned its partitions, short enough to fail fast. */
    private static final Duration ARRIVAL_DEADLINE = Duration.ofSeconds(30);

    private static KafkaTemplate<String, byte[]> producer;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @AfterAll
    static void closeProducer() {
        if (producer != null) {
            producer.destroy();
            producer = null;
        }
    }

    /**
     * A producer that writes raw bytes, mirroring how the Flink sink publishes: the service must
     * cope with whatever {@link JsonCodec} produces, not with something a typed serializer
     * reshaped on the way out.
     */
    private KafkaTemplate<String, byte[]> producer() {
        if (producer == null) {
            Map<String, Object> config = KafkaTestUtils.producerProps(broker);
            config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            ProducerFactory<String, byte[]> factory = new DefaultKafkaProducerFactory<>(config);
            producer = new KafkaTemplate<>(factory);
        }
        return producer;
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void anAlertPublishedToTheTopicIsPersisted() throws Exception {
        Alert alert = wireAlert("WIRE-1", SensorType.RAINFALL, Severity.EXTREME);

        publish(Topics.GENERATED_ALERTS, alert.getStationId(), JsonCodec.toBytes(alert));

        AlertEntity stored = awaitAlert(alert.getAlertId());

        assertThat(stored.getStationId()).isEqualTo("WIRE-1");
        assertThat(stored.getStationName()).isEqualTo("Station WIRE-1");
        assertThat(stored.getDistrictId()).isEqualTo("DIST-01");
        assertThat(stored.getSensorType()).isEqualTo(SensorType.RAINFALL);
        assertThat(stored.getSeverity()).isEqualTo(Severity.EXTREME);
        assertThat(stored.getSeverityRank()).isEqualTo(Severity.EXTREME.rank());
        assertThat(stored.getUnit()).isEqualTo(alert.getUnit());
        assertThat(stored.getObservedValue()).isEqualTo(alert.getObservedValue());
        assertThat(stored.getThresholdValue()).isEqualTo(alert.getThresholdValue());
        // The instants are the point of the exercise: they must survive the ISO-8601 round trip
        // to the exact second, or the pipeline-latency measurement is meaningless.
        assertThat(stored.getWindowStart()).isEqualTo(alert.getWindowStart());
        assertThat(stored.getWindowEnd()).isEqualTo(alert.getWindowEnd());
        assertThat(stored.getGeneratedAt()).isEqualTo(alert.getGeneratedAt());
        assertThat(stored.getReceivedAt()).isNotNull();
        assertThat(stored.isAcknowledged()).isFalse();
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void thePersistedAlertIsImmediatelyVisibleThroughTheQueryApi() throws Exception {
        Alert alert = wireAlert("WIRE-2", SensorType.RIVER_LEVEL, Severity.SEVERE);

        publish(Topics.GENERATED_ALERTS, alert.getStationId(), JsonCodec.toBytes(alert));
        awaitAlert(alert.getAlertId());

        mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                        .get("/api/v1/alerts/{id}", alert.getAlertId()))
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers
                        .status().isOk())
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers
                        .jsonPath("$.stationId").value("WIRE-2"));
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void republishingTheSameAlertDoesNotCreateASecondRow() throws Exception {
        // The at-least-once replay, exercised through the real broker rather than in-process.
        Alert alert = wireAlert("WIRE-3", SensorType.RESERVOIR_LEVEL, Severity.WARNING);
        byte[] payload = JsonCodec.toBytes(alert);

        publish(Topics.GENERATED_ALERTS, alert.getStationId(), payload);
        awaitAlert(alert.getAlertId());

        publish(Topics.GENERATED_ALERTS, alert.getStationId(), payload);
        publish(Topics.GENERATED_ALERTS, alert.getStationId(), payload);

        // Nothing to wait for on the happy path, so assert the invariant holds and keeps holding.
        await().during(Duration.ofSeconds(2)).atMost(ARRIVAL_DEADLINE)
                .untilAsserted(() -> assertThat(alertRepository.count()).isEqualTo(1L));
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void anAggregatePublishedToTheObservabilityTopicIsPersisted() throws Exception {
        StationWindowAggregate aggregate = AlertFixtures.aggregate(
                "WIRE-4", SensorType.RAINFALL, Instant.now().minusSeconds(600).truncatedTo(
                        java.time.temporal.ChronoUnit.SECONDS), 61.5d);

        publish(Topics.AGGREGATED_TELEMETRY, aggregate.getStationId(),
                JsonCodec.toBytes(aggregate));

        await().atMost(ARRIVAL_DEADLINE).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(aggregateRepository.count()).isEqualTo(1L));

        assertThat(aggregateRepository.findAll().getFirst())
                .satisfies(row -> {
                    assertThat(row.getKey().getStationId()).isEqualTo("WIRE-4");
                    assertThat(row.getKey().getSensorType()).isEqualTo(SensorType.RAINFALL);
                    assertThat(row.getKey().getWindowStart()).isEqualTo(aggregate.getWindowStart());
                    assertThat(row.getAggregatedValue()).isEqualTo(61.5d);
                    assertThat(row.getReadingCount()).isEqualTo(12L);
                });
    }

    /**
     * Builds an alert whose window has just closed, so the pipeline-latency metric sees a
     * realistic - and positive - gap rather than the fixture's fixed calendar date.
     */
    private static Alert wireAlert(String stationId, SensorType sensorType, Severity severity) {
        Instant windowStart = Instant.now()
                .minus(AlertFixtures.WINDOW_SIZE)
                .truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
        return AlertFixtures.alert(stationId, sensorType, severity, windowStart, "DIST-01");
    }

    private void publish(String topic, String key, byte[] payload) throws Exception {
        producer().send(topic, key, payload).get(15, TimeUnit.SECONDS);
    }

    private AlertEntity awaitAlert(String alertId) {
        await().atMost(ARRIVAL_DEADLINE).pollInterval(Duration.ofMillis(200))
                .until(() -> alertRepository.findById(alertId).isPresent());
        return alertRepository.findById(alertId).orElseThrow();
    }
}
