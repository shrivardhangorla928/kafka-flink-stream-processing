package com.stream.processing.alert.mapper;

import com.stream.processing.alert.AlertFixtures;
import com.stream.processing.alert.domain.StationWindowAggregateEntity;
import com.stream.processing.alert.domain.StationWindowKey;
import com.stream.processing.alert.web.dto.StationWindowDto;
import com.stream.processing.common.SensorType;
import com.stream.processing.common.StationWindowAggregate;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the aggregate translations and the natural key they upsert on. */
class StationWindowAggregateMapperTest {

    private final StationWindowAggregateMapper mapper = new StationWindowAggregateMapper();

    @Test
    void keyIsStationSensorTypeAndWindowStart() {
        StationWindowAggregate aggregate =
                AlertFixtures.aggregate("STN-1", SensorType.RAINFALL, AlertFixtures.WINDOW_START, 42d);

        StationWindowKey key = mapper.keyOf(aggregate);

        assertThat(key).isEqualTo(new StationWindowKey(
                "STN-1", SensorType.RAINFALL, AlertFixtures.WINDOW_START));
        // Value semantics matter: without them a replay would look like a different row.
        assertThat(key).hasSameHashCodeAs(new StationWindowKey(
                "STN-1", SensorType.RAINFALL, AlertFixtures.WINDOW_START));
    }

    @Test
    void toEntityCopiesEveryStatistic() {
        StationWindowAggregate aggregate =
                AlertFixtures.aggregate("STN-1", SensorType.RAINFALL, AlertFixtures.WINDOW_START, 42d);
        Instant receivedAt = Instant.parse("2026-08-11T06:05:10Z");

        StationWindowAggregateEntity entity = mapper.toEntity(aggregate, receivedAt);

        assertThat(entity.getKey().getStationId()).isEqualTo("STN-1");
        assertThat(entity.getWindowEnd()).isEqualTo(aggregate.getWindowEnd());
        assertThat(entity.getReadingCount()).isEqualTo(12L);
        assertThat(entity.getSum()).isEqualTo(42d);
        assertThat(entity.getMin()).isEqualTo(0.5d);
        assertThat(entity.getMax()).isEqualTo(42d);
        assertThat(entity.getAvg()).isEqualTo(42d / 12d);
        assertThat(entity.getAggregatedValue()).isEqualTo(42d);
        assertThat(entity.getUnit()).isEqualTo(SensorType.RAINFALL.getUnit());
        assertThat(entity.getComputedAt()).isEqualTo(aggregate.getComputedAt());
        assertThat(entity.getReceivedAt()).isEqualTo(receivedAt);
    }

    @Test
    void missingWindowEndFallsBackToTheWindowStartBecauseTheColumnIsNotNull() {
        StationWindowAggregate aggregate =
                AlertFixtures.aggregate("STN-1", SensorType.RAINFALL, AlertFixtures.WINDOW_START, 1d);
        aggregate.setWindowEnd(null);

        StationWindowAggregateEntity entity = mapper.toEntity(aggregate, Instant.EPOCH);

        assertThat(entity.getWindowEnd()).isEqualTo(AlertFixtures.WINDOW_START);
    }

    @Test
    void updateAdvancesReceivedAtBecauseAggregatesCarryNoOperatorState() {
        StationWindowAggregate aggregate =
                AlertFixtures.aggregate("STN-1", SensorType.RAINFALL, AlertFixtures.WINDOW_START, 1d);
        StationWindowAggregateEntity entity = mapper.toEntity(aggregate, Instant.EPOCH);

        StationWindowAggregate replay =
                AlertFixtures.aggregate("STN-1", SensorType.RAINFALL, AlertFixtures.WINDOW_START, 9d);
        Instant later = Instant.parse("2026-08-11T07:00:00Z");
        mapper.updateEntity(entity, replay, later);

        assertThat(entity.getAggregatedValue()).isEqualTo(9d);
        assertThat(entity.getReceivedAt()).isEqualTo(later);
    }

    @Test
    void toDtoFlattensTheCompositeKey() {
        StationWindowAggregate aggregate =
                AlertFixtures.aggregate("STN-3", SensorType.RIVER_LEVEL, AlertFixtures.WINDOW_START, 5d);

        StationWindowDto dto = mapper.toDto(mapper.toEntity(aggregate, Instant.EPOCH));

        assertThat(dto.stationId()).isEqualTo("STN-3");
        assertThat(dto.sensorType()).isEqualTo(SensorType.RIVER_LEVEL);
        assertThat(dto.windowStart()).isEqualTo(AlertFixtures.WINDOW_START);
        assertThat(dto.aggregatedValue()).isEqualTo(5d);
    }
}
