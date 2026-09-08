package com.stream.processing.alert.service;

import com.stream.processing.alert.AlertFixtures;
import com.stream.processing.alert.domain.AlertEntity;
import com.stream.processing.alert.mapper.AlertMapper;
import com.stream.processing.alert.repository.AlertRepository;
import com.stream.processing.common.Alert;
import com.stream.processing.common.SensorType;
import com.stream.processing.common.Severity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The idempotency contract, exercised against a real database.
 *
 * <p>This is the test that stands behind the claim in {@link com.stream.processing.common.AlertIds}:
 * the upstream sink is at-least-once, so the same deterministic id will arrive again, and it must
 * collapse onto the row that is already there rather than duplicating a flood warning.</p>
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("test")
@Import({AlertPersistenceService.class, AlertMapper.class})
class AlertPersistenceServiceTest {

    private static final Instant RECEIVED_AT = Instant.parse("2026-08-11T06:05:03Z");

    @Autowired
    private AlertPersistenceService service;

    @Autowired
    private AlertRepository repository;

    @BeforeEach
    void clean() {
        repository.deleteAll();
    }

    @Test
    void persistsANewBatch() {
        List<Alert> batch = List.of(
                AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.WARNING),
                AlertFixtures.alert("STN-2", SensorType.RIVER_LEVEL, Severity.SEVERE));

        PersistResult result = service.persist(batch, RECEIVED_AT);

        assertThat(result.inserted()).isEqualTo(2);
        assertThat(result.duplicates()).isZero();
        assertThat(result.received()).isEqualTo(2);
        assertThat(repository.count()).isEqualTo(2);
    }

    @Test
    void replayingTheSameBatchInsertsNothingAndReportsDuplicates() {
        List<Alert> batch = List.of(
                AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.WARNING),
                AlertFixtures.alert("STN-2", SensorType.RIVER_LEVEL, Severity.SEVERE));
        service.persist(batch, RECEIVED_AT);

        PersistResult replay = service.persist(batch, RECEIVED_AT.plusSeconds(3600));

        assertThat(replay.inserted()).isZero();
        assertThat(replay.duplicates()).isEqualTo(2);
        assertThat(replay.insertedIds()).isEmpty();
        assertThat(repository.count()).isEqualTo(2);
    }

    @Test
    void aRepeatWithinOneBatchIsCollapsedRatherThanWrittenTwice() {
        Alert alert = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.WARNING);

        PersistResult result = service.persist(List.of(alert, alert, alert), RECEIVED_AT);

        assertThat(result.received()).isEqualTo(3);
        assertThat(result.inserted()).isEqualTo(1);
        assertThat(result.duplicates()).isEqualTo(2);
        assertThat(repository.count()).isEqualTo(1);
    }

    @Test
    void aBatchMixingNewAndReplayedAlertsReportsBothCorrectly() {
        Alert first = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.WARNING);
        service.persist(List.of(first), RECEIVED_AT);

        Alert second = AlertFixtures.alert("STN-2", SensorType.RIVER_LEVEL, Severity.SEVERE);
        PersistResult result = service.persist(List.of(first, second), RECEIVED_AT);

        assertThat(result.inserted()).isEqualTo(1);
        assertThat(result.isNew(second.getAlertId())).isTrue();
        assertThat(result.isNew(first.getAlertId())).isFalse();
        assertThat(result.duplicates()).isEqualTo(1);
        assertThat(repository.count()).isEqualTo(2);
    }

    @Test
    void aReplayRefreshesTheProducerFieldsButKeepsReceivedAtAndTheAcknowledgement() {
        Alert alert = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.WARNING);
        service.persist(List.of(alert), RECEIVED_AT);

        AlertEntity stored = repository.findById(alert.getAlertId()).orElseThrow();
        stored.acknowledge("duty-officer", Instant.parse("2026-08-11T06:30:00Z"));
        repository.saveAndFlush(stored);

        Alert corrected = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.WARNING);
        corrected.setObservedValue(123.4d);
        corrected.setMessage("corrected reading");
        service.persist(List.of(corrected), RECEIVED_AT.plusSeconds(7200));

        AlertEntity after = repository.findById(alert.getAlertId()).orElseThrow();
        assertThat(after.getObservedValue()).isEqualTo(123.4d);
        assertThat(after.getMessage()).isEqualTo("corrected reading");
        assertThat(after.getReceivedAt()).isEqualTo(RECEIVED_AT);
        assertThat(after.isAcknowledged()).isTrue();
        assertThat(after.getAcknowledgedBy()).isEqualTo("duty-officer");
    }

    @Test
    void anAlertWithNoIdIsDroppedBecauseItCannotBeMadeIdempotent() {
        Alert usable = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.WARNING);
        Alert unusable = AlertFixtures.alert("STN-2", SensorType.RAINFALL, Severity.WARNING);
        unusable.setAlertId("  ");

        PersistResult result = service.persist(List.of(usable, unusable), RECEIVED_AT);

        assertThat(result.inserted()).isEqualTo(1);
        // Counted as a duplicate rather than silently vanishing: received minus inserted.
        assertThat(result.duplicates()).isEqualTo(1);
        assertThat(repository.count()).isEqualTo(1);
    }

    @Test
    void aBatchOfOnlyUnusableAlertsWritesNothing() {
        Alert unusable = AlertFixtures.alert("STN-2", SensorType.RAINFALL, Severity.WARNING);
        unusable.setAlertId(null);

        PersistResult result = service.persist(List.of(unusable), RECEIVED_AT);

        assertThat(result.inserted()).isZero();
        assertThat(result.received()).isEqualTo(1);
        assertThat(repository.count()).isZero();
    }

    @Test
    void anEmptyBatchIsANoOp() {
        PersistResult result = service.persist(List.of(), RECEIVED_AT);

        assertThat(result.received()).isZero();
        assertThat(result.inserted()).isZero();
        assertThat(result.duplicates()).isZero();
    }

    @Test
    void differentSeverityBandsForTheSameWindowAreDistinctAlerts() {
        // AlertIds folds severity into the id, so an escalating window legitimately produces a
        // second alert rather than overwriting the first. Guards against over-eager de-duplication.
        Alert warning = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.WARNING);
        Alert severe = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.SEVERE);

        PersistResult result = service.persist(List.of(warning, severe), RECEIVED_AT);

        assertThat(result.inserted()).isEqualTo(2);
        assertThat(repository.count()).isEqualTo(2);
    }
}
