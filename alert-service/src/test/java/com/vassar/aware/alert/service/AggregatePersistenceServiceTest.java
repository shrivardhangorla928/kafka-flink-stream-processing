package com.vassar.aware.alert.service;

import com.vassar.aware.alert.AlertFixtures;
import com.vassar.aware.alert.domain.StationWindowAggregateEntity;
import com.vassar.aware.alert.domain.StationWindowKey;
import com.vassar.aware.alert.mapper.StationWindowAggregateMapper;
import com.vassar.aware.alert.repository.StationWindowAggregateRepository;
import com.vassar.aware.common.SensorType;
import com.vassar.aware.common.StationWindowAggregate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Upsert-on-natural-key behaviour of the observability feed. */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("test")
@Import({AggregatePersistenceService.class, StationWindowAggregateMapper.class})
class AggregatePersistenceServiceTest {

    private static final Instant RECEIVED_AT = Instant.parse("2026-08-11T06:05:10Z");

    @Autowired
    private AggregatePersistenceService service;

    @Autowired
    private StationWindowAggregateRepository repository;

    @BeforeEach
    void clean() {
        repository.deleteAll();
    }

    @Test
    void writesEachDistinctWindowOnce() {
        List<StationWindowAggregate> batch = List.of(
                AlertFixtures.aggregate("STN-1", SensorType.RAINFALL, AlertFixtures.WINDOW_START, 10d),
                AlertFixtures.aggregate("STN-1", SensorType.RAINFALL,
                        AlertFixtures.WINDOW_START.plus(AlertFixtures.WINDOW_SIZE), 20d));

        assertThat(service.persist(batch, RECEIVED_AT)).isEqualTo(2);
        assertThat(repository.count()).isEqualTo(2);
    }

    @Test
    void replayingAWindowOverwritesItRatherThanAppendingHistory() {
        service.persist(List.of(AlertFixtures.aggregate(
                "STN-1", SensorType.RAINFALL, AlertFixtures.WINDOW_START, 10d)), RECEIVED_AT);

        service.persist(List.of(AlertFixtures.aggregate(
                        "STN-1", SensorType.RAINFALL, AlertFixtures.WINDOW_START, 33d)),
                RECEIVED_AT.plusSeconds(3600));

        assertThat(repository.count()).isEqualTo(1);
        StationWindowAggregateEntity stored = repository.findById(new StationWindowKey(
                "STN-1", SensorType.RAINFALL, AlertFixtures.WINDOW_START)).orElseThrow();
        assertThat(stored.getAggregatedValue()).isEqualTo(33d);
        assertThat(stored.getReceivedAt()).isEqualTo(RECEIVED_AT.plusSeconds(3600));
    }

    @Test
    void theSameStationAndWindowUnderDifferentSensorTypesAreDifferentRows() {
        service.persist(List.of(
                AlertFixtures.aggregate("STN-1", SensorType.RAINFALL, AlertFixtures.WINDOW_START, 10d),
                AlertFixtures.aggregate("STN-1", SensorType.RIVER_LEVEL, AlertFixtures.WINDOW_START, 4d)
        ), RECEIVED_AT);

        assertThat(repository.count()).isEqualTo(2);
    }

    @Test
    void aggregatesWithAnIncompleteNaturalKeyAreDiscarded() {
        StationWindowAggregate noStation =
                AlertFixtures.aggregate("STN-1", SensorType.RAINFALL, AlertFixtures.WINDOW_START, 1d);
        noStation.setStationId(null);
        StationWindowAggregate noSensorType =
                AlertFixtures.aggregate("STN-2", SensorType.RAINFALL, AlertFixtures.WINDOW_START, 1d);
        noSensorType.setSensorType(null);
        StationWindowAggregate noWindow =
                AlertFixtures.aggregate("STN-3", SensorType.RAINFALL, AlertFixtures.WINDOW_START, 1d);
        noWindow.setWindowStart(null);

        assertThat(service.persist(List.of(noStation, noSensorType, noWindow), RECEIVED_AT)).isZero();
        assertThat(repository.count()).isZero();
    }

    @Test
    void anEmptyBatchIsANoOp() {
        assertThat(service.persist(List.of(), RECEIVED_AT)).isZero();
    }

    @Test
    void timelineFinderReturnsNewestWindowsFirst() {
        service.persist(List.of(
                AlertFixtures.aggregate("STN-1", SensorType.RAINFALL, AlertFixtures.WINDOW_START, 1d),
                AlertFixtures.aggregate("STN-1", SensorType.RAINFALL,
                        AlertFixtures.WINDOW_START.plusSeconds(300), 2d),
                AlertFixtures.aggregate("STN-1", SensorType.RAINFALL,
                        AlertFixtures.WINDOW_START.plusSeconds(600), 3d)), RECEIVED_AT);

        List<StationWindowAggregateEntity> recent =
                repository.findByKeyStationIdOrderByKeyWindowStartDesc("STN-1", PageRequest.ofSize(2));

        assertThat(recent).extracting(StationWindowAggregateEntity::getAggregatedValue)
                .containsExactly(3d, 2d);
    }
}
