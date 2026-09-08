package com.stream.processing.alert.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stream.processing.alert.AlertFixtures;
import com.stream.processing.alert.service.AlertMetrics;
import com.stream.processing.alert.service.AlertPersistenceService;
import com.stream.processing.alert.service.PersistResult;
import com.stream.processing.common.Alert;
import com.stream.processing.common.JsonCodec;
import com.stream.processing.common.SensorType;
import com.stream.processing.common.Severity;
import com.stream.processing.common.Topics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.search.Search;
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
import java.util.Set;
import java.util.concurrent.TimeUnit;

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

/**
 * Unit tests for the alert listener's delivery guarantees and instrumentation.
 *
 * <p>The persistence layer is mocked because what is under test here is the ordering - persist,
 * then measure, then acknowledge - and the failure path where the acknowledgement must not
 * happen. The metrics registry is real: asserting on a {@code SimpleMeterRegistry} verifies the
 * actual meter names and tags that the Grafana dashboards query, which a mocked registry
 * would not.</p>
 */
class AlertConsumerTest {

    private final AlertPersistenceService persistenceService = mock(AlertPersistenceService.class);
    private final Acknowledgment acknowledgment = mock(Acknowledgment.class);
    private final MeterRegistry registry = new SimpleMeterRegistry();
    private final ObjectMapper objectMapper = JsonCodec.create();

    private AlertMetrics metrics;
    private AlertConsumer consumer;

    @BeforeEach
    void setUp() {
        metrics = new AlertMetrics(registry);
        consumer = new AlertConsumer(persistenceService, metrics, objectMapper);
    }

    @Test
    void persistsTheBatchAndOnlyThenAcknowledges() {
        Alert first = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.WARNING);
        Alert second = AlertFixtures.alert("STN-2", SensorType.RIVER_LEVEL, Severity.SEVERE);
        given(allNew(first, second));

        consumer.onAlerts(records(first, second), acknowledgment);

        InOrder ordered = inOrder(persistenceService, acknowledgment);
        ordered.verify(persistenceService).persist(anyList(), any(Instant.class));
        ordered.verify(acknowledgment).acknowledge();
        ordered.verifyNoMoreInteractions();
    }

    @Test
    void aFailedBatchIsNotAcknowledgedSoTheOffsetsStayPut() {
        Alert alert = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.WARNING);
        when(persistenceService.persist(anyList(), any(Instant.class)))
                .thenThrow(new IllegalStateException("connection pool exhausted"));

        assertThatThrownBy(() -> consumer.onAlerts(records(alert), acknowledgment))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("connection pool exhausted");

        verify(acknowledgment, never()).acknowledge();
        assertThat(counter(AlertMetrics.PERSIST_FAILURES).count()).isEqualTo(1.0d);
    }

    @Test
    void aDuplicateIncrementsTheDuplicateCounterAndNotTheConsumedCounter() {
        Alert alert = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.WARNING);
        // The store already had it: nothing inserted, one record received.
        when(persistenceService.persist(anyList(), any(Instant.class)))
                .thenReturn(new PersistResult(Set.of(), 1));

        consumer.onAlerts(records(alert), acknowledgment);

        assertThat(counter(AlertMetrics.DUPLICATE).count()).isEqualTo(1.0d);
        assertThat(Search.in(registry).name(AlertMetrics.CONSUMED).counters()).isEmpty();
        // A replay is still fully handled, so its offset must be committed.
        verify(acknowledgment).acknowledge();
    }

    @Test
    void aMixedBatchCountsEachRecordExactlyOnce() {
        Alert stored = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.WARNING);
        Alert fresh = AlertFixtures.alert("STN-2", SensorType.RIVER_LEVEL, Severity.EXTREME);
        when(persistenceService.persist(anyList(), any(Instant.class)))
                .thenReturn(new PersistResult(Set.of(fresh.getAlertId()), 2));

        consumer.onAlerts(records(stored, fresh), acknowledgment);

        assertThat(counter(AlertMetrics.DUPLICATE).count()).isEqualTo(1.0d);
        assertThat(consumedCounter(Severity.EXTREME, SensorType.RIVER_LEVEL).count()).isEqualTo(1.0d);
        assertThat(Search.in(registry).name(AlertMetrics.CONSUMED).counters()).hasSize(1);
    }

    @Test
    void theConsumedCounterIsTaggedBySeverityAndSensorType() {
        Alert rain = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.WARNING);
        Alert river = AlertFixtures.alert("STN-2", SensorType.RIVER_LEVEL, Severity.EXTREME);
        given(allNew(rain, river));

        consumer.onAlerts(records(rain, river), acknowledgment);

        assertThat(consumedCounter(Severity.WARNING, SensorType.RAINFALL).count()).isEqualTo(1.0d);
        assertThat(consumedCounter(Severity.EXTREME, SensorType.RIVER_LEVEL).count()).isEqualTo(1.0d);
    }

    @Test
    void persistenceLatencyIsTimed() {
        Alert alert = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.WARNING);
        given(allNew(alert));

        consumer.onAlerts(records(alert), acknowledgment);

        Timer timer = registry.find(AlertMetrics.PERSIST_LATENCY).timer();
        assertThat(timer).isNotNull();
        assertThat(timer.count()).isEqualTo(1L);
    }

    @Test
    void endToEndPipelineLatencyIsMeasuredFromTheWindowEnd() {
        // The headline evaluation metric: window close to arrival in the store. Anchored to a
        // window that closed 30 seconds ago so the assertion does not depend on the wall clock.
        Alert alert = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.SEVERE);
        Instant windowEnd = Instant.now().minusSeconds(30);
        alert.setWindowStart(windowEnd.minus(AlertFixtures.WINDOW_SIZE));
        alert.setWindowEnd(windowEnd);
        given(allNew(alert));

        consumer.onAlerts(records(alert), acknowledgment);

        Timer timer = registry.find(AlertMetrics.PIPELINE_LATENCY).timer();
        assertThat(timer).isNotNull();
        assertThat(timer.count()).isEqualTo(1L);
        assertThat(timer.totalTime(TimeUnit.SECONDS)).isBetween(29.0d, 120.0d);
    }

    @Test
    void aReplayIsExcludedFromThePipelineLatencyHistogram() {
        Alert alert = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.SEVERE);
        when(persistenceService.persist(anyList(), any(Instant.class)))
                .thenReturn(new PersistResult(Set.of(), 1));

        consumer.onAlerts(records(alert), acknowledgment);

        assertThat(registry.find(AlertMetrics.PIPELINE_LATENCY).timer().count()).isZero();
    }

    @Test
    void aWindowEndInTheFutureIsDroppedRatherThanRecordedAsZero() {
        // Clamping a producer's clock skew to zero would quietly drag the median down.
        Alert alert = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.SEVERE);
        alert.setWindowEnd(Instant.now().plusSeconds(600));
        given(allNew(alert));

        consumer.onAlerts(records(alert), acknowledgment);

        assertThat(registry.find(AlertMetrics.PIPELINE_LATENCY).timer().count()).isZero();
        assertThat(consumedCounter(Severity.SEVERE, SensorType.RAINFALL).count()).isEqualTo(1.0d);
    }

    @Test
    void anUnparseableRecordFailsTheBatchAtItsExactIndex() {
        Alert good = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.WARNING);
        List<ConsumerRecord<String, byte[]>> batch = List.of(
                record(0L, JsonCodec.toBytes(good)),
                record(1L, "{ this is not json".getBytes(StandardCharsets.UTF_8)));

        assertThatThrownBy(() -> consumer.onAlerts(batch, acknowledgment))
                .isInstanceOf(BatchListenerFailedException.class)
                .extracting(e -> ((BatchListenerFailedException) e).getIndex())
                .isEqualTo(1);

        // Nothing was written and nothing was committed; the error handler will dead-letter
        // record 1 and redeliver the rest.
        verifyNoInteractions(persistenceService);
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void aNullValuedRecordIsSkippedInsteadOfWedgingTheBatch() {
        Alert good = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.WARNING);
        given(allNew(good));

        consumer.onAlerts(List.of(record(0L, null), record(1L, JsonCodec.toBytes(good))), acknowledgment);

        verify(acknowledgment).acknowledge();
        assertThat(consumedCounter(Severity.WARNING, SensorType.RAINFALL).count()).isEqualTo(1.0d);
    }

    @Test
    void anEmptyBatchIsAcknowledgedWithoutTouchingTheDatabase() {
        consumer.onAlerts(List.of(), acknowledgment);

        verify(acknowledgment).acknowledge();
        verifyNoInteractions(persistenceService);
    }

    @Test
    void thePayloadIsDecodedWithTheSharedWireFormat() {
        Alert alert = AlertFixtures.alert("STN-5", SensorType.RESERVOIR_LEVEL, Severity.EXTREME);
        given(allNew(alert));

        consumer.onAlerts(records(alert), acknowledgment);

        // Instants survive the ISO-8601 round trip the pipeline uses, so the latency measurement
        // is anchored to the producer's window and not to a mangled timestamp.
        assertThat(consumedCounter(Severity.EXTREME, SensorType.RESERVOIR_LEVEL).count())
                .isEqualTo(1.0d);
    }

    private void given(PersistResult result) {
        when(persistenceService.persist(anyList(), any(Instant.class))).thenReturn(result);
    }

    private static PersistResult allNew(Alert... alerts) {
        return new PersistResult(
                java.util.Arrays.stream(alerts).map(Alert::getAlertId).collect(java.util.stream.Collectors.toSet()),
                alerts.length);
    }

    private static List<ConsumerRecord<String, byte[]>> records(Alert... alerts) {
        List<ConsumerRecord<String, byte[]>> batch = new java.util.ArrayList<>(alerts.length);
        for (int i = 0; i < alerts.length; i++) {
            batch.add(record(i, JsonCodec.toBytes(alerts[i])));
        }
        return batch;
    }

    private static ConsumerRecord<String, byte[]> record(long offset, byte[] payload) {
        return new ConsumerRecord<>(Topics.GENERATED_ALERTS, 0, offset, "STN", payload);
    }

    private Counter counter(String name) {
        Counter counter = registry.find(name).counter();
        assertThat(counter).as("counter %s", name).isNotNull();
        return counter;
    }

    private Counter consumedCounter(Severity severity, SensorType sensorType) {
        Counter counter = registry.find(AlertMetrics.CONSUMED)
                .tag("severity", severity.name())
                .tag("sensor_type", sensorType.name())
                .counter();
        assertThat(counter).as("consumed counter for %s/%s", severity, sensorType).isNotNull();
        return counter;
    }
}
