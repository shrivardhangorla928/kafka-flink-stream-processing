package com.vassar.aware.ingest.publish;

import com.vassar.aware.common.SensorReading;
import com.vassar.aware.common.Topics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * The single exit from this service onto Kafka.
 *
 * <p>Everything published goes through here so that three cross-cutting rules hold no matter
 * whether a reading came from the simulator or from a REST caller: it is keyed by station id, so
 * all of a station's readings land on one partition and the downstream keyed windows see them in
 * order; it is validated before it is written, so a malformed record is diverted to the
 * dead-letter topic instead of stalling the Flink job; and it is counted and timed, so the
 * Prometheus dashboards in the monitoring chapter have a per-sensor-type publish rate.</p>
 *
 * <p>Sends are not awaited. The producer is configured for durability with unbounded retries
 * inside a 120 s delivery timeout, so blocking the scrape thread on each acknowledgement would
 * turn a transient broker hiccup into a missed scrape round; the outcome is recorded on the
 * callback instead.</p>
 */
@Component
public class TelemetryPublisher {

    /**
     * Micrometer names use dots; the Prometheus registry renders them as
     * {@code aware_ingest_published_total} and {@code aware_ingest_publish_failures_total}.
     * Naming them with underscores here would produce a doubled {@code _total_total} suffix.
     */
    public static final String PUBLISHED_COUNTER = "aware.ingest.published";
    public static final String FAILURES_COUNTER = "aware.ingest.publish.failures";
    public static final String PUBLISH_TIMER = "aware.ingest.publish.duration";

    /** Header carrying why a record was dead-lettered, so the DLQ is triageable without replaying it. */
    public static final String DLQ_REASON_HEADER = "aware-dlq-reason";

    private static final String TAG_SENSOR_TYPE = "sensor.type";
    private static final String TAG_TOPIC = "topic";
    private static final String UNKNOWN = "unknown";

    private static final Logger LOG = LoggerFactory.getLogger(TelemetryPublisher.class);

    private final KafkaTemplate<String, SensorReading> kafkaTemplate;
    private final MeterRegistry meterRegistry;

    public TelemetryPublisher(KafkaTemplate<String, SensorReading> kafkaTemplate, MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Stamps the publication time and sends the reading to the raw topic, or to the dead-letter
     * topic if it cannot be keyed, windowed or measured.
     *
     * <p>{@code ingestedAt} is set here rather than by the caller because it is defined as the
     * moment the record was handed to Kafka; the difference from {@code eventTime} is the scrape
     * lag the evaluation measures.</p>
     */
    public CompletableFuture<SendResult<String, SensorReading>> publish(SensorReading reading) {
        Objects.requireNonNull(reading, "reading");
        reading.setIngestedAt(Instant.now());

        String rejection = rejectionReason(reading);
        boolean valid = rejection == null;
        String topic = valid ? Topics.RAW_TELEMETRY : Topics.DEAD_LETTER;

        ProducerRecord<String, SensorReading> record = new ProducerRecord<>(topic, keyOf(reading), reading);
        if (!valid) {
            record.headers().add(DLQ_REASON_HEADER, rejection.getBytes(StandardCharsets.UTF_8));
            LOG.warn("Dead-lettering reading {} from station {}: {}",
                    reading.getReadingId(), reading.getStationId(), rejection);
        }

        Timer.Sample sample = Timer.start(meterRegistry);
        CompletableFuture<SendResult<String, SensorReading>> future = kafkaTemplate.send(record);
        String sensorType = sensorTypeTag(reading);
        future.whenComplete((result, error) -> {
            sample.stop(publishTimer(topic, sensorType));
            if (error == null) {
                publishedCounter(topic, sensorType).increment();
            } else {
                failureCounter(topic, sensorType).increment();
                LOG.error("Failed to publish reading {} for station {} to {}",
                        reading.getReadingId(), reading.getStationId(), topic, error);
            }
        });
        return future;
    }

    /**
     * Publishes a whole scrape round.
     *
     * @return the number of readings handed to the producer; because sends are asynchronous this
     *         is a submission count, and the {@code publish.failures} counter is what says how
     *         many of them the broker never acknowledged
     */
    public int publishAll(Collection<SensorReading> readings) {
        if (readings == null || readings.isEmpty()) {
            return 0;
        }
        int submitted = 0;
        for (SensorReading reading : readings) {
            publish(reading);
            submitted++;
        }
        return submitted;
    }

    /**
     * Explains why {@link SensorReading#isValid()} would reject a reading, or {@code null} if it
     * would not. Duplicating the checks would risk drift, so the flag is asked first and the
     * fields are only inspected to produce a human-readable reason.
     */
    private static String rejectionReason(SensorReading reading) {
        if (reading.isValid()) {
            return null;
        }
        if (reading.getStationId() == null || reading.getStationId().isBlank()) {
            return "missing stationId";
        }
        if (reading.getSensorType() == null) {
            return "missing sensorType";
        }
        if (reading.getEventTime() == null) {
            return "missing eventTime";
        }
        double value = reading.getValue();
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return "value is not a finite number";
        }
        if (value < 0.0d) {
            return "value is negative";
        }
        return "failed validation";
    }

    /**
     * Keys by station so a station's readings stay on one partition and therefore in order.
     * A dead-lettered record may have no station id at all; falling back to the reading id keeps
     * the DLQ evenly spread instead of piling every malformed record onto one partition.
     */
    private static String keyOf(SensorReading reading) {
        if (reading.getStationId() != null && !reading.getStationId().isBlank()) {
            return reading.getStationId();
        }
        return reading.getReadingId();
    }

    private static String sensorTypeTag(SensorReading reading) {
        return reading.getSensorType() == null ? UNKNOWN : reading.getSensorType().name();
    }

    private Counter publishedCounter(String topic, String sensorType) {
        return Counter.builder(PUBLISHED_COUNTER)
                .description("Sensor readings handed to Kafka and acknowledged by the broker")
                .tag(TAG_TOPIC, topic)
                .tag(TAG_SENSOR_TYPE, sensorType)
                .register(meterRegistry);
    }

    private Counter failureCounter(String topic, String sensorType) {
        return Counter.builder(FAILURES_COUNTER)
                .description("Sensor readings the broker never acknowledged")
                .tag(TAG_TOPIC, topic)
                .tag(TAG_SENSOR_TYPE, sensorType)
                .register(meterRegistry);
    }

    private Timer publishTimer(String topic, String sensorType) {
        return Timer.builder(PUBLISH_TIMER)
                .description("Time from handing a reading to the producer until the broker acknowledged it")
                .tag(TAG_TOPIC, topic)
                .tag(TAG_SENSOR_TYPE, sensorType)
                .register(meterRegistry);
    }
}
