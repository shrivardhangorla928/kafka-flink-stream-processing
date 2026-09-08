package com.stream.processing.alert.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stream.processing.alert.service.AggregatePersistenceService;
import com.stream.processing.alert.service.AlertMetrics;
import com.stream.processing.common.StationWindowAggregate;
import com.stream.processing.common.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Consumes {@link Topics#AGGREGATED_TELEMETRY} into the windowed-aggregate table.
 *
 * <p>This is the observability feed, not the alerting path: it exists so the dissertation can
 * show what every window computed alongside the few that tripped a threshold. It is switchable
 * off through {@code stream.alert.consume-aggregates.enabled} because it carries roughly two
 * orders of magnitude more traffic than the alert topic, and during a load test the database
 * write budget belongs to the alerts.</p>
 *
 * <p>Switched off via {@code autoStartup} rather than by removing the bean: the container is
 * registered either way, so an operator can start it at runtime through the actuator endpoint
 * without a redeploy.</p>
 */
@Component
public class AggregateConsumer {

    private static final Logger log = LoggerFactory.getLogger(AggregateConsumer.class);

    private final AggregatePersistenceService persistenceService;
    private final AlertMetrics metrics;
    private final ObjectMapper objectMapper;

    public AggregateConsumer(AggregatePersistenceService persistenceService,
                             AlertMetrics metrics,
                             ObjectMapper objectMapper) {
        this.persistenceService = persistenceService;
        this.metrics = metrics;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            id = "stream-aggregate-listener",
            topics = Topics.AGGREGATED_TELEMETRY,
            groupId = "alert-service-aggregates",
            containerFactory = "aggregateListenerContainerFactory",
            autoStartup = "${stream.alert.consume-aggregates.enabled:true}")
    public void onAggregates(List<ConsumerRecord<String, byte[]>> records,
                             Acknowledgment acknowledgment) {
        if (records.isEmpty()) {
            acknowledgment.acknowledge();
            return;
        }

        Instant receivedAt = Instant.now();
        List<StationWindowAggregate> aggregates = parse(records);

        int written;
        try {
            written = persistenceService.persist(aggregates, receivedAt);
        } catch (RuntimeException e) {
            metrics.persistFailure();
            log.error("Failed to persist batch of {} aggregates; offsets left uncommitted",
                    aggregates.size(), e);
            throw e;
        }

        metrics.aggregatesConsumed(written);
        acknowledgment.acknowledge();
    }

    private List<StationWindowAggregate> parse(List<ConsumerRecord<String, byte[]>> records) {
        List<StationWindowAggregate> aggregates = new ArrayList<>(records.size());
        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<String, byte[]> record = records.get(i);
            if (record.value() == null) {
                continue;
            }
            try {
                aggregates.add(objectMapper.readValue(record.value(), StationWindowAggregate.class));
            } catch (Exception e) {
                throw new BatchListenerFailedException(
                        "Unparseable aggregate payload at " + record.topic() + '-' + record.partition()
                                + " offset " + record.offset(), e, i);
            }
        }
        return aggregates;
    }
}
