package com.vassar.aware.alert.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vassar.aware.alert.service.AlertMetrics;
import com.vassar.aware.alert.service.AlertPersistenceService;
import com.vassar.aware.alert.service.PersistResult;
import com.vassar.aware.common.Alert;
import com.vassar.aware.common.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Consumes {@link Topics#GENERATED_ALERTS} and writes it into the alert store.
 *
 * <p>The whole design of this listener follows from one fact stated in
 * {@link com.vassar.aware.common.AlertIds}: the upstream sink is at-least-once, so the same
 * {@code alertId} will arrive more than once. That makes reprocessing safe, which in turn makes
 * it correct to acknowledge only after the transaction commits - the failure mode becomes
 * "process twice", which the upsert absorbs, instead of "lose an alert", which it cannot.</p>
 *
 * <p>Batch listener rather than record-by-record: at 500 records a poll, one transaction and one
 * commit per batch is roughly two orders of magnitude fewer round trips than one per alert, which
 * is what lets a single replica keep up during the load tests in the evaluation chapter.</p>
 */
@Component
public class AlertConsumer {

    private static final Logger log = LoggerFactory.getLogger(AlertConsumer.class);

    private final AlertPersistenceService persistenceService;
    private final AlertMetrics metrics;
    private final ObjectMapper objectMapper;

    public AlertConsumer(AlertPersistenceService persistenceService,
                         AlertMetrics metrics,
                         ObjectMapper objectMapper) {
        this.persistenceService = persistenceService;
        this.metrics = metrics;
        this.objectMapper = objectMapper;
    }

    /**
     * Handles one poll batch: parse, persist in a single transaction, then acknowledge.
     *
     * <p>The ordering is the contract. If persistence throws, this method throws too and the
     * acknowledgement never happens, so the container's error handler retries the batch and the
     * offsets stay where they were. A batch that failed must never look committed.</p>
     *
     * @throws BatchListenerFailedException naming the exact index of an unparseable record, so
     *         the error handler can dead-letter that one record instead of the whole batch
     */
    @KafkaListener(
            topics = Topics.GENERATED_ALERTS,
            groupId = "alert-service",
            containerFactory = "alertListenerContainerFactory")
    public void onAlerts(List<ConsumerRecord<String, byte[]>> records, Acknowledgment acknowledgment) {
        if (records.isEmpty()) {
            acknowledgment.acknowledge();
            return;
        }

        Instant receivedAt = Instant.now();
        List<Alert> alerts = parse(records);

        long startedAt = System.nanoTime();
        PersistResult result;
        try {
            result = persistenceService.persist(alerts, receivedAt);
        } catch (RuntimeException e) {
            metrics.persistFailure();
            log.error("Failed to persist batch of {} alerts; offsets left uncommitted for retry",
                    alerts.size(), e);
            throw e;
        }
        metrics.persistLatency(Duration.ofNanos(System.nanoTime() - startedAt));

        recordOutcome(alerts, result, receivedAt);

        // Only now. Everything above either succeeded or threw.
        acknowledgment.acknowledge();
    }

    private List<Alert> parse(List<ConsumerRecord<String, byte[]>> records) {
        List<Alert> alerts = new ArrayList<>(records.size());
        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<String, byte[]> record = records.get(i);
            if (record.value() == null) {
                // A tombstone on an alert topic is meaningless; skipping keeps the batch moving
                // rather than dead-lettering a record that carries nothing to diagnose.
                log.warn("Skipping null-valued record at {}-{} offset {}",
                        record.topic(), record.partition(), record.offset());
                continue;
            }
            try {
                alerts.add(objectMapper.readValue(record.value(), Alert.class));
            } catch (Exception e) {
                // Index-carrying exception: the error handler commits everything before this
                // record, dead-letters this one, and redelivers the remainder. Without the index
                // one bad payload would take its whole batch to the DLQ with it.
                throw new BatchListenerFailedException(
                        "Unparseable alert payload at " + record.topic() + '-' + record.partition()
                                + " offset " + record.offset(), e, i);
            }
        }
        return alerts;
    }

    /**
     * Feeds the metrics that the evaluation chapter reads.
     *
     * <p>Replays are counted as duplicates and deliberately excluded from both the consumed
     * counter and the latency histogram: a re-emitted window is not a new alert, and its age is a
     * measure of how long ago Flink restarted rather than of how fast the pipeline is.</p>
     */
    private void recordOutcome(List<Alert> alerts, PersistResult result, Instant receivedAt) {
        for (Alert alert : alerts) {
            if (result.isNew(alert.getAlertId())) {
                metrics.alertConsumed(alert.getSeverity(), alert.getSensorType());
                metrics.pipelineLatency(alert, receivedAt);
            }
        }
        metrics.duplicates(result.duplicates());

        if (log.isDebugEnabled()) {
            log.debug("Consumed batch: {} received, {} new, {} duplicate",
                    result.received(), result.inserted(), result.duplicates());
        }
    }
}
