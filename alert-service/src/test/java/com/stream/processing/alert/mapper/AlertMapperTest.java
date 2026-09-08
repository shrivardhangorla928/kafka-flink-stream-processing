package com.stream.processing.alert.mapper;

import com.stream.processing.alert.AlertFixtures;
import com.stream.processing.alert.domain.AlertEntity;
import com.stream.processing.alert.web.dto.AlertDto;
import com.stream.processing.common.Alert;
import com.stream.processing.common.SensorType;
import com.stream.processing.common.Severity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the event/entity/DTO translations, including the degenerate inputs. */
class AlertMapperTest {

    private final AlertMapper mapper = new AlertMapper();

    @Test
    void toEntityCopiesEveryEventField() {
        Alert alert = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.SEVERE);
        Instant receivedAt = Instant.parse("2026-08-11T06:05:03Z");

        AlertEntity entity = mapper.toEntity(alert, receivedAt);

        assertThat(entity.getAlertId()).isEqualTo(alert.getAlertId());
        assertThat(entity.getStationId()).isEqualTo("STN-1");
        assertThat(entity.getStationName()).isEqualTo("Station STN-1");
        assertThat(entity.getDistrictId()).isEqualTo("DIST-01");
        assertThat(entity.getSensorType()).isEqualTo(SensorType.RAINFALL);
        assertThat(entity.getSeverity()).isEqualTo(Severity.SEVERE);
        assertThat(entity.getMetric()).isEqualTo(alert.getMetric());
        assertThat(entity.getObservedValue()).isEqualTo(87.5d);
        assertThat(entity.getThresholdValue()).isEqualTo(60.0d);
        assertThat(entity.getUnit()).isEqualTo(alert.getUnit());
        assertThat(entity.getWindowStart()).isEqualTo(alert.getWindowStart());
        assertThat(entity.getWindowEnd()).isEqualTo(alert.getWindowEnd());
        assertThat(entity.getMessage()).isEqualTo(alert.getMessage());
        assertThat(entity.getLatitude()).isEqualTo(16.5062d);
        assertThat(entity.getLongitude()).isEqualTo(80.6480d);
        assertThat(entity.getGeneratedAt()).isEqualTo(alert.getGeneratedAt());
        assertThat(entity.getReceivedAt()).isEqualTo(receivedAt);
        assertThat(entity.isAcknowledged()).isFalse();
    }

    @ParameterizedTest
    @EnumSource(Severity.class)
    void severityRankIsDerivedFromTheSeverity(Severity severity) {
        Alert alert = AlertFixtures.alert("STN-1", SensorType.RIVER_LEVEL, severity);

        AlertEntity entity = mapper.toEntity(alert, Instant.now());

        assertThat(entity.getSeverityRank()).isEqualTo(severity.rank());
    }

    @Test
    void missingSeverityFallsBackToInfoRatherThanFailingTheBatch() {
        Alert alert = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.WARNING);
        alert.setSeverity(null);

        AlertEntity entity = mapper.toEntity(alert, Instant.now());

        assertThat(entity.getSeverity()).isEqualTo(Severity.INFO);
        assertThat(entity.getSeverityRank()).isZero();
    }

    @Test
    void missingGeneratedAtFallsBackToTheWindowEnd() {
        Alert alert = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.WARNING);
        alert.setGeneratedAt(null);

        AlertEntity entity = mapper.toEntity(alert, Instant.now());

        assertThat(entity.getGeneratedAt()).isEqualTo(alert.getWindowEnd());
    }

    @Test
    void anAlertWithNoTimestampsAtAllStillProducesAStorableRow() {
        Alert alert = new Alert();
        alert.setAlertId("orphan");
        alert.setStationId("STN-1");
        alert.setSensorType(SensorType.RAINFALL);

        AlertEntity entity = mapper.toEntity(alert, Instant.EPOCH);

        assertThat(entity.getGeneratedAt()).isEqualTo(Instant.EPOCH);
        assertThat(entity.getWindowStart()).isNull();
        assertThat(entity.getWindowEnd()).isNull();
        assertThat(entity.getUnit()).isNull();
        assertThat(entity.getMessage()).isNull();
    }

    @Test
    void updateEntityRefreshesProducerFieldsButNeverTheAcknowledgement() {
        Alert original = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.WARNING);
        AlertEntity entity = mapper.toEntity(original, Instant.parse("2026-08-11T06:05:00Z"));
        entity.acknowledge("duty-officer", Instant.parse("2026-08-11T06:10:00Z"));

        Alert replayed = AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.WARNING);
        replayed.setObservedValue(99.9d);
        replayed.setMessage("corrected");
        mapper.updateEntity(entity, replayed);

        assertThat(entity.getObservedValue()).isEqualTo(99.9d);
        assertThat(entity.getMessage()).isEqualTo("corrected");
        // A checkpoint replay must not resurrect an alert an operator has already handled.
        assertThat(entity.isAcknowledged()).isTrue();
        assertThat(entity.getAcknowledgedBy()).isEqualTo("duty-officer");
        assertThat(entity.getAcknowledgedAt()).isEqualTo(Instant.parse("2026-08-11T06:10:00Z"));
        assertThat(entity.getReceivedAt()).isEqualTo(Instant.parse("2026-08-11T06:05:00Z"));
    }

    @Test
    void eventRoundTripsThroughTheEntityUnchanged() {
        Alert alert = AlertFixtures.alert("STN-7", SensorType.RESERVOIR_LEVEL, Severity.EXTREME);

        Alert roundTripped = mapper.toEvent(mapper.toEntity(alert, Instant.now()));

        assertThat(roundTripped).usingRecursiveComparison().isEqualTo(alert);
    }

    @Test
    void toDtoExposesStoredStateAndHidesTheIndexingDetail() {
        Alert alert = AlertFixtures.alert("STN-2", SensorType.RIVER_LEVEL, Severity.EXTREME);
        AlertEntity entity = mapper.toEntity(alert, Instant.parse("2026-08-11T06:05:04Z"));
        entity.acknowledge("ops", Instant.parse("2026-08-11T07:00:00Z"));

        AlertDto dto = mapper.toDto(entity);

        assertThat(dto.alertId()).isEqualTo(alert.getAlertId());
        assertThat(dto.severity()).isEqualTo(Severity.EXTREME);
        assertThat(dto.receivedAt()).isEqualTo(Instant.parse("2026-08-11T06:05:04Z"));
        assertThat(dto.acknowledged()).isTrue();
        assertThat(dto.acknowledgedBy()).isEqualTo("ops");
        // severityRank is a storage concern; the record has no component for it at all.
        assertThat(AlertDto.class.getRecordComponents())
                .noneMatch(component -> component.getName().equals("severityRank"));
    }

    @Test
    void acknowledgingTwiceKeepsTheFirstResponder() {
        AlertEntity entity = mapper.toEntity(
                AlertFixtures.alert("STN-1", SensorType.RAINFALL, Severity.INFO), Instant.EPOCH);

        boolean first = entity.acknowledge("first", Instant.parse("2026-08-11T06:00:00Z"));
        boolean second = entity.acknowledge("second", Instant.parse("2026-08-11T09:00:00Z"));

        assertThat(first).isTrue();
        assertThat(second).isFalse();
        assertThat(entity.getAcknowledgedBy()).isEqualTo("first");
        assertThat(entity.getAcknowledgedAt()).isEqualTo(Instant.parse("2026-08-11T06:00:00Z"));
    }
}
