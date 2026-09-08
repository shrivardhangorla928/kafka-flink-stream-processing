package com.stream.processing.ingest.publish;

import com.stream.processing.common.SensorReading;
import com.stream.processing.common.SensorType;
import com.stream.processing.common.Topics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The publisher is the one place where routing, keying and instrumentation are decided, so these
 * tests are the contract between the ingest service and everything downstream of the topic.
 */
class TelemetryPublisherTest {

    private static final Instant EVENT_TIME = Instant.parse("2024-01-01T00:00:00Z");

    private KafkaTemplate<String, SensorReading> kafkaTemplate;
    private MeterRegistry meterRegistry;
    private TelemetryPublisher publisher;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        meterRegistry = new SimpleMeterRegistry();
        publisher = new TelemetryPublisher(kafkaTemplate, meterRegistry);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenAnswer(invocation ->
                CompletableFuture.completedFuture(acknowledge(invocation.getArgument(0))));
    }

    private static SendResult<String, SensorReading> acknowledge(ProducerRecord<String, SensorReading> record) {
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition(record.topic(), 0), 0L, 0, System.currentTimeMillis(), 0, 0);
        return new SendResult<>(record, metadata);
    }

    private static SensorReading validReading() {
        SensorReading reading = new SensorReading();
        reading.setReadingId("r-1");
        reading.setStationId("STN-0001");
        reading.setStationName("Vijayawada Rain Gauge");
        reading.setDistrictId("AP-NTR");
        reading.setSensorType(SensorType.RAINFALL);
        reading.setUnit(SensorType.RAINFALL.getUnit());
        reading.setValue(7.5d);
        reading.setLatitude(16.51d);
        reading.setLongitude(80.62d);
        reading.setEventTime(EVENT_TIME);
        reading.setSource("STREAM_SIMULATOR");
        return reading;
    }

    @SuppressWarnings("unchecked")
    private ProducerRecord<String, SensorReading> capturedRecord() {
        org.mockito.ArgumentCaptor<ProducerRecord<String, SensorReading>> captor =
                org.mockito.ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        return captor.getValue();
    }

    @Test
    @DisplayName("a valid reading goes to the raw topic keyed by station id")
    void validReadingGoesToTheRawTopic() {
        publisher.publish(validReading());

        ProducerRecord<String, SensorReading> record = capturedRecord();
        assertThat(record.topic()).isEqualTo(Topics.RAW_TELEMETRY);
        assertThat(record.key()).isEqualTo("STN-0001");
        assertThat(record.headers().lastHeader(TelemetryPublisher.DLQ_REASON_HEADER)).isNull();
        assertThat(record.value().getValue()).isEqualTo(7.5d);
    }

    @Test
    @DisplayName("publication stamps ingestedAt, which is what the scrape-lag measurement uses")
    void publicationStampsIngestedAt() {
        SensorReading reading = validReading();
        assertThat(reading.getIngestedAt()).isNull();

        publisher.publish(reading);

        assertThat(reading.getIngestedAt()).isNotNull();
        assertThat(reading.getIngestedAt()).isAfter(reading.getEventTime());
    }

    @Test
    @DisplayName("a reading with no station id is dead-lettered with the reason attached")
    void readingWithoutStationIdIsDeadLettered() {
        SensorReading reading = validReading();
        reading.setStationId(null);

        publisher.publish(reading);

        ProducerRecord<String, SensorReading> record = capturedRecord();
        assertThat(record.topic()).isEqualTo(Topics.DEAD_LETTER);
        assertThat(record.key()).as("falls back to the reading id so the DLQ stays spread").isEqualTo("r-1");
        assertThat(reasonOf(record)).isEqualTo("missing stationId");
    }

    @Test
    @DisplayName("a negative value is dead-lettered rather than published as telemetry")
    void negativeValueIsDeadLettered() {
        SensorReading reading = validReading();
        reading.setValue(-0.1d);

        publisher.publish(reading);

        ProducerRecord<String, SensorReading> record = capturedRecord();
        assertThat(record.topic()).isEqualTo(Topics.DEAD_LETTER);
        assertThat(reasonOf(record)).isEqualTo("value is negative");
    }

    @Test
    @DisplayName("every other kind of invalid reading is dead-lettered with its own reason")
    void otherInvalidReadingsAreDeadLettered() {
        assertThat(reasonFor(reading -> reading.setSensorType(null))).isEqualTo("missing sensorType");
        assertThat(reasonFor(reading -> reading.setEventTime(null))).isEqualTo("missing eventTime");
        assertThat(reasonFor(reading -> reading.setValue(Double.NaN)))
                .isEqualTo("value is not a finite number");
        assertThat(reasonFor(reading -> reading.setValue(Double.POSITIVE_INFINITY)))
                .isEqualTo("value is not a finite number");
        assertThat(reasonFor(reading -> reading.setStationId("   "))).isEqualTo("missing stationId");
    }

    @Test
    @DisplayName("acknowledged publications are counted per topic and sensor type")
    void publishedReadingsAreCounted() {
        publisher.publish(validReading());
        SensorReading river = validReading();
        river.setSensorType(SensorType.RIVER_LEVEL);
        publisher.publish(river);

        assertThat(counter(TelemetryPublisher.PUBLISHED_COUNTER, Topics.RAW_TELEMETRY, "RAINFALL"))
                .isEqualTo(1.0d);
        assertThat(counter(TelemetryPublisher.PUBLISHED_COUNTER, Topics.RAW_TELEMETRY, "RIVER_LEVEL"))
                .isEqualTo(1.0d);
        assertThat(meterRegistry.find(TelemetryPublisher.FAILURES_COUNTER).counters()).isEmpty();
    }

    @Test
    @DisplayName("a dead-lettered reading with no sensor type is still counted, tagged unknown")
    void deadLetteredReadingIsCounted() {
        SensorReading reading = validReading();
        reading.setSensorType(null);

        publisher.publish(reading);

        assertThat(counter(TelemetryPublisher.PUBLISHED_COUNTER, Topics.DEAD_LETTER, "unknown"))
                .isEqualTo(1.0d);
    }

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("a send the broker never acknowledges increments the failure counter")
    void failedSendIncrementsTheFailureCounter() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unreachable")));

        CompletableFuture<?> future = publisher.publish(validReading());

        assertThat(future).isCompletedExceptionally();
        assertThat(counter(TelemetryPublisher.FAILURES_COUNTER, Topics.RAW_TELEMETRY, "RAINFALL"))
                .isEqualTo(1.0d);
        assertThat(meterRegistry.find(TelemetryPublisher.PUBLISHED_COUNTER).counters()).isEmpty();
    }

    @Test
    @DisplayName("publish latency is timed per topic and sensor type")
    void publishLatencyIsTimed() {
        publisher.publish(validReading());

        assertThat(meterRegistry.get(TelemetryPublisher.PUBLISH_TIMER)
                .tag("topic", Topics.RAW_TELEMETRY)
                .tag("sensor.type", "RAINFALL")
                .timer().count()).isEqualTo(1L);
    }

    @Test
    @DisplayName("a batch is submitted reading by reading and its size returned")
    void publishAllSubmitsEveryReading() {
        assertThat(publisher.publishAll(List.of(validReading(), validReading(), validReading())))
                .isEqualTo(3);
        verify(kafkaTemplate, times(3)).send(any(ProducerRecord.class));
    }

    @Test
    @DisplayName("an empty or absent batch is a no-op")
    void publishAllToleratesEmptyBatches() {
        assertThat(publisher.publishAll(List.of())).isZero();
        assertThat(publisher.publishAll(null)).isZero();
        verify(kafkaTemplate, times(0)).send(any(ProducerRecord.class));
    }

    @Test
    @DisplayName("publishing nothing at all is a programming error, not a silent no-op")
    void publishRejectsNull() {
        assertThatThrownBy(() -> publisher.publish(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("the counters scrape as stream_ingest_published_total and stream_ingest_publish_failures_total")
    void metricsExposeThePrometheusNamesTheDashboardsUse() {
        PrometheusMeterRegistry prometheus = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        TelemetryPublisher instrumented = new TelemetryPublisher(kafkaTemplate, prometheus);

        instrumented.publish(validReading());

        String scrape = prometheus.scrape();
        assertThat(scrape).contains("stream_ingest_published_total");
        assertThat(scrape).contains("sensor_type=\"RAINFALL\"");
        assertThat(scrape).contains("topic=\"" + Topics.RAW_TELEMETRY + "\"");
    }

    private double counter(String name, String topic, String sensorType) {
        return meterRegistry.get(name).tag("topic", topic).tag("sensor.type", sensorType).counter().count();
    }

    private String reasonFor(java.util.function.Consumer<SensorReading> corruption) {
        KafkaTemplate<String, SensorReading> template = freshTemplate();
        TelemetryPublisher isolated = new TelemetryPublisher(template, new SimpleMeterRegistry());
        SensorReading reading = validReading();
        corruption.accept(reading);

        isolated.publish(reading);

        @SuppressWarnings("unchecked")
        org.mockito.ArgumentCaptor<ProducerRecord<String, SensorReading>> captor =
                org.mockito.ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo(Topics.DEAD_LETTER);
        return reasonOf(captor.getValue());
    }

    @SuppressWarnings("unchecked")
    private KafkaTemplate<String, SensorReading> freshTemplate() {
        KafkaTemplate<String, SensorReading> template = mock(KafkaTemplate.class);
        when(template.send(any(ProducerRecord.class))).thenAnswer(invocation ->
                CompletableFuture.completedFuture(acknowledge(invocation.getArgument(0))));
        return template;
    }

    private static String reasonOf(ProducerRecord<String, SensorReading> record) {
        return new String(record.headers().lastHeader(TelemetryPublisher.DLQ_REASON_HEADER).value(),
                StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("the returned future completes once the broker acknowledges")
    void returnedFutureCompletesOnAcknowledgement() throws Exception {
        SendResult<String, SensorReading> result =
                publisher.publish(validReading()).get(5, TimeUnit.SECONDS);

        assertThat(result.getRecordMetadata().topic()).isEqualTo(Topics.RAW_TELEMETRY);
    }
}
