package com.vassar.aware.alert.kafka;

import com.vassar.aware.alert.AlertFixtures;
import com.vassar.aware.alert.service.AggregatePersistenceService;
import com.vassar.aware.alert.service.AlertMetrics;
import com.vassar.aware.common.JsonCodec;
import com.vassar.aware.common.SensorType;
import com.vassar.aware.common.StationWindowAggregate;
import com.vassar.aware.common.Topics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.Acknowledgment;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** The observability feed's listener obeys the same persist-then-acknowledge contract. */
class AggregateConsumerTest {

    private final AggregatePersistenceService persistenceService =
            mock(AggregatePersistenceService.class);
    private final Acknowledgment acknowledgment = mock(Acknowledgment.class);
    private final MeterRegistry registry = new SimpleMeterRegistry();

    private AggregateConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new AggregateConsumer(
                persistenceService, new AlertMetrics(registry), JsonCodec.create());
    }

    @Test
    void persistsThenAcknowledgesAndCountsTheRowsWritten() {
        when(persistenceService.persist(anyList(), any(Instant.class))).thenReturn(2);

        consumer.onAggregates(List.of(
                record(0L, JsonCodec.toBytes(AlertFixtures.aggregate(
                        "STN-1", SensorType.RAINFALL, AlertFixtures.WINDOW_START, 10d))),
                record(1L, JsonCodec.toBytes(AlertFixtures.aggregate(
                        "STN-2", SensorType.RIVER_LEVEL, AlertFixtures.WINDOW_START, 3d)))),
                acknowledgment);

        InOrder ordered = inOrder(persistenceService, acknowledgment);
        ordered.verify(persistenceService).persist(anyList(), any(Instant.class));
        ordered.verify(acknowledgment).acknowledge();

        assertThat(registry.find(AlertMetrics.AGGREGATES_CONSUMED).counter().count()).isEqualTo(2.0d);
    }

    @Test
    void aFailedBatchIsNotAcknowledged() {
        StationWindowAggregate aggregate = AlertFixtures.aggregate(
                "STN-1", SensorType.RAINFALL, AlertFixtures.WINDOW_START, 10d);
        when(persistenceService.persist(anyList(), any(Instant.class)))
                .thenThrow(new IllegalStateException("deadlock detected"));

        assertThatThrownBy(() -> consumer.onAggregates(
                List.of(record(0L, JsonCodec.toBytes(aggregate))), acknowledgment))
                .isInstanceOf(IllegalStateException.class);

        verify(acknowledgment, never()).acknowledge();
        assertThat(registry.find(AlertMetrics.PERSIST_FAILURES).counter().count()).isEqualTo(1.0d);
    }

    @Test
    void anUnparseableAggregateFailsTheBatchAtItsExactIndex() {
        List<ConsumerRecord<String, byte[]>> batch = List.of(
                record(0L, JsonCodec.toBytes(AlertFixtures.aggregate(
                        "STN-1", SensorType.RAINFALL, AlertFixtures.WINDOW_START, 10d))),
                record(1L, "<xml/>".getBytes(StandardCharsets.UTF_8)));

        assertThatThrownBy(() -> consumer.onAggregates(batch, acknowledgment))
                .isInstanceOf(BatchListenerFailedException.class)
                .extracting(e -> ((BatchListenerFailedException) e).getIndex())
                .isEqualTo(1);

        verifyNoInteractions(persistenceService);
    }

    @Test
    void anEmptyBatchIsAcknowledgedWithoutTouchingTheDatabase() {
        consumer.onAggregates(List.of(), acknowledgment);

        verify(acknowledgment).acknowledge();
        verifyNoInteractions(persistenceService);
    }

    private static ConsumerRecord<String, byte[]> record(long offset, byte[] payload) {
        return new ConsumerRecord<>(Topics.AGGREGATED_TELEMETRY, 0, offset, "STN", payload);
    }
}
