package com.stream.processing.alert.repository;

import com.stream.processing.alert.AlertFixtures;
import com.stream.processing.alert.domain.AlertEntity;
import com.stream.processing.alert.mapper.AlertMapper;
import com.stream.processing.common.Alert;
import com.stream.processing.common.SensorType;
import com.stream.processing.common.Severity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Persistence tests against the real Flyway migrations.
 *
 * <p>{@code replace = NONE} keeps Boot from substituting its own throwaway embedded database:
 * these tests must run against the H2 instance configured in PostgreSQL mode by the {@code test}
 * profile, with the production {@code V1__alerts.sql} applied and {@code ddl-auto=validate}
 * checking the entity mappings against it. Anything less would be testing a schema that is never
 * deployed.</p>
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("test")
class AlertRepositoryTest {

    private static final Instant T0 = Instant.parse("2026-08-11T06:00:00Z");

    @Autowired
    private AlertRepository repository;

    private final AlertMapper mapper = new AlertMapper();

    @BeforeEach
    void seed() {
        repository.deleteAll();
        repository.saveAllAndFlush(List.of(
                row("STN-1", SensorType.RAINFALL, Severity.INFO, "DIST-01", T0, false),
                row("STN-1", SensorType.RAINFALL, Severity.WARNING, "DIST-01", T0.plusSeconds(60), false),
                row("STN-2", SensorType.RIVER_LEVEL, Severity.SEVERE, "DIST-01", T0.plusSeconds(120), true),
                row("STN-3", SensorType.RESERVOIR_LEVEL, Severity.EXTREME, "DIST-02", T0.plusSeconds(180), false),
                row("STN-4", SensorType.RAINFALL, Severity.WARNING, "DIST-02", T0.plusSeconds(240), false)));
    }

    @Test
    void storesAndReadsBackAnAlert() {
        Alert alert = AlertFixtures.alert("STN-9", SensorType.RAINFALL, Severity.SEVERE);
        repository.saveAndFlush(mapper.toEntity(alert, Instant.parse("2026-08-11T06:05:03Z")));

        AlertEntity stored = repository.findById(alert.getAlertId()).orElseThrow();

        assertThat(stored.getStationId()).isEqualTo("STN-9");
        assertThat(stored.getSeverity()).isEqualTo(Severity.SEVERE);
        assertThat(stored.getSeverityRank()).isEqualTo(Severity.SEVERE.rank());
        assertThat(stored.getUnit()).isEqualTo(SensorType.RAINFALL.getUnit());
        assertThat(stored.getReceivedAt()).isEqualTo(Instant.parse("2026-08-11T06:05:03Z"));
        assertThat(stored.isAcknowledged()).isFalse();
    }

    @Test
    void savingTheSameAlertIdTwiceUpdatesTheRowInsteadOfAddingOne() {
        Alert alert = AlertFixtures.alert("STN-9", SensorType.RAINFALL, Severity.SEVERE);
        long before = repository.count();

        repository.saveAndFlush(mapper.toEntity(alert, Instant.EPOCH));
        repository.saveAndFlush(mapper.toEntity(alert, Instant.EPOCH));

        assertThat(repository.count()).isEqualTo(before + 1);
    }

    @Test
    void findExistingIdsReturnsOnlyTheStoredOnes() {
        String stored = repository.findAll().getFirst().getAlertId();

        List<String> existing = repository.findExistingIds(List.of(stored, "not-stored"));

        assertThat(existing).containsExactly(stored);
    }

    @Test
    void findExistingIdsOnAnAllNewBatchReturnsNothing() {
        assertThat(repository.findExistingIds(List.of("a", "b", "c"))).isEmpty();
    }

    @Test
    void filtersByDistrict() {
        Page<AlertEntity> page = search(new AlertFilter(
                "DIST-02", null, null, null, null, null, null));

        assertThat(page.getContent())
                .extracting(AlertEntity::getStationId)
                .containsExactlyInAnyOrder("STN-3", "STN-4");
    }

    @Test
    void filtersByStationAndSensorType() {
        Page<AlertEntity> page = search(new AlertFilter(
                null, "STN-1", SensorType.RAINFALL, null, null, null, null));

        assertThat(page.getTotalElements()).isEqualTo(2);
    }

    @Test
    void severityFilterIsAMinimumNotAnExactMatch() {
        Page<AlertEntity> page = search(new AlertFilter(
                null, null, null, Severity.WARNING, null, null, null));

        assertThat(page.getContent())
                .extracting(AlertEntity::getSeverity)
                .containsExactlyInAnyOrder(
                        Severity.WARNING, Severity.SEVERE, Severity.EXTREME, Severity.WARNING);
        assertThat(page.getContent()).noneMatch(row -> row.getSeverity() == Severity.INFO);
    }

    @Test
    void highestSeverityBandMatchesOnlyItself() {
        Page<AlertEntity> page = search(new AlertFilter(
                null, null, null, Severity.EXTREME, null, null, null));

        assertThat(page.getContent()).singleElement()
                .extracting(AlertEntity::getStationId).isEqualTo("STN-3");
    }

    @Test
    void timeRangeIsLowerInclusiveAndUpperExclusive() {
        Page<AlertEntity> page = search(new AlertFilter(
                null, null, null, null, T0.plusSeconds(60), T0.plusSeconds(180), null));

        assertThat(page.getContent())
                .extracting(AlertEntity::getGeneratedAt)
                .containsExactlyInAnyOrder(T0.plusSeconds(60), T0.plusSeconds(120));
    }

    @Test
    void filtersByAcknowledgementState() {
        assertThat(search(new AlertFilter(null, null, null, null, null, null, true))
                .getTotalElements()).isEqualTo(1);
        assertThat(search(new AlertFilter(null, null, null, null, null, null, false))
                .getTotalElements()).isEqualTo(4);
    }

    @Test
    void filtersCompose() {
        Page<AlertEntity> page = search(new AlertFilter(
                "DIST-02", null, SensorType.RAINFALL, Severity.WARNING,
                T0, T0.plusSeconds(600), false));

        assertThat(page.getContent()).singleElement()
                .extracting(AlertEntity::getStationId).isEqualTo("STN-4");
    }

    @Test
    void anEmptyFilterMatchesEverything() {
        assertThat(search(AlertFilter.none()).getTotalElements()).isEqualTo(5);
    }

    @Test
    void defaultOrderingIsNewestFirst() {
        Page<AlertEntity> page = repository.findAll(
                AlertSpecifications.matching(AlertFilter.none()),
                PageRequest.of(0, 10, Sort.by(Sort.Direction.DESC, "generatedAt")));

        assertThat(page.getContent())
                .extracting(AlertEntity::getGeneratedAt)
                .isSortedAccordingTo(java.util.Comparator.reverseOrder());
    }

    @Test
    void summaryCountsBySeverityOverTheRange() {
        Map<Severity, Long> bySeverity = repository.countBySeverity(T0, T0.plusSeconds(300))
                .stream()
                .collect(Collectors.toMap(AlertRepository.SeverityCount::getSeverity,
                        AlertRepository.SeverityCount::getTotal));

        assertThat(bySeverity).containsOnly(
                Map.entry(Severity.INFO, 1L),
                Map.entry(Severity.WARNING, 2L),
                Map.entry(Severity.SEVERE, 1L),
                Map.entry(Severity.EXTREME, 1L));
    }

    @Test
    void summaryCountsByDistrictOverTheRange() {
        Map<String, Long> byDistrict = repository.countByDistrict(T0, T0.plusSeconds(300))
                .stream()
                .collect(Collectors.toMap(AlertRepository.DistrictCount::getDistrictId,
                        AlertRepository.DistrictCount::getTotal));

        assertThat(byDistrict).containsOnly(Map.entry("DIST-01", 3L), Map.entry("DIST-02", 2L));
    }

    @Test
    void summaryRangeExcludesAlertsOutsideIt() {
        assertThat(repository.countInRange(T0, T0.plusSeconds(120))).isEqualTo(2);
        assertThat(repository.countInRange(T0.plusSeconds(1000), T0.plusSeconds(2000))).isZero();
    }

    private Page<AlertEntity> search(AlertFilter filter) {
        return repository.findAll(AlertSpecifications.matching(filter), PageRequest.of(0, 50));
    }

    private AlertEntity row(String stationId,
                            SensorType sensorType,
                            Severity severity,
                            String districtId,
                            Instant generatedAt,
                            boolean acknowledged) {
        Alert alert = AlertFixtures.alert(stationId, sensorType, severity, generatedAt, districtId);
        AlertEntity entity = mapper.toEntity(alert, generatedAt.plusSeconds(1));
        entity.setGeneratedAt(generatedAt);
        if (acknowledged) {
            entity.acknowledge("ops", generatedAt.plusSeconds(30));
        }
        return entity;
    }

    /** Guards the assumption the other tests rest on: fixtures produce distinct ids. */
    @Test
    void fixtureRowsHaveDistinctDeterministicIds() {
        Function<AlertEntity, String> id = AlertEntity::getAlertId;
        assertThat(repository.findAll()).extracting(id).doesNotHaveDuplicates().hasSize(5);
    }
}
