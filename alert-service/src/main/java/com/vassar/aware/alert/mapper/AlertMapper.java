package com.vassar.aware.alert.mapper;

import com.vassar.aware.alert.domain.AlertEntity;
import com.vassar.aware.alert.web.dto.AlertDto;
import com.vassar.aware.common.Alert;
import com.vassar.aware.common.Severity;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Translates between the three shapes an alert takes: the wire event {@link Alert}, the row
 * {@link AlertEntity} and the response {@link AlertDto}.
 *
 * <p>A component rather than a static utility so the consumer and the controller receive it by
 * constructor injection and can be unit-tested with a real instance.</p>
 */
@Component
public class AlertMapper {

    /**
     * Builds a fresh row from an inbound event.
     *
     * @param receivedAt the moment this service took delivery; supplied by the caller rather than
     *                   read from the clock here so the consumer stamps a whole batch with one
     *                   consistent instant and the tests stay deterministic
     */
    public AlertEntity toEntity(Alert alert, Instant receivedAt) {
        AlertEntity entity = new AlertEntity(alert.getAlertId());
        copyEventFields(alert, entity);
        entity.setReceivedAt(receivedAt);
        return entity;
    }

    /**
     * Refreshes an existing row from a replayed event.
     *
     * <p>Only the fields the producer owns are copied. {@code receivedAt} and the acknowledgement
     * trio are this service's own state: a checkpoint replay of a window an operator already
     * acknowledged must not resurrect it as unacknowledged.</p>
     */
    public void updateEntity(AlertEntity entity, Alert alert) {
        copyEventFields(alert, entity);
    }

    private void copyEventFields(Alert alert, AlertEntity entity) {
        entity.setStationId(alert.getStationId());
        entity.setStationName(alert.getStationName());
        entity.setDistrictId(alert.getDistrictId());
        entity.setSensorType(alert.getSensorType());
        entity.setSeverity(alert.getSeverity() == null ? Severity.INFO : alert.getSeverity());
        entity.setMetric(alert.getMetric());
        entity.setObservedValue(alert.getObservedValue());
        entity.setThresholdValue(alert.getThresholdValue());
        entity.setUnit(alert.getUnit());
        entity.setWindowStart(alert.getWindowStart());
        entity.setWindowEnd(alert.getWindowEnd());
        entity.setMessage(alert.getMessage());
        entity.setLatitude(alert.getLatitude());
        entity.setLongitude(alert.getLongitude());
        // generatedAt is NOT NULL and is the API's sort key; an event that somehow arrives without
        // one is anchored to the window it describes rather than rejected, because losing a real
        // flood warning to a missing timestamp is the worse failure.
        entity.setGeneratedAt(firstNonNull(alert.getGeneratedAt(), alert.getWindowEnd(), Instant.EPOCH));
    }

    /** Reverses the mapping, for tests and for any future replay-from-store path. */
    public Alert toEvent(AlertEntity entity) {
        Alert alert = new Alert();
        alert.setAlertId(entity.getAlertId());
        alert.setStationId(entity.getStationId());
        alert.setStationName(entity.getStationName());
        alert.setDistrictId(entity.getDistrictId());
        alert.setSensorType(entity.getSensorType());
        alert.setSeverity(entity.getSeverity());
        alert.setMetric(entity.getMetric());
        alert.setObservedValue(entity.getObservedValue());
        alert.setThresholdValue(entity.getThresholdValue());
        alert.setUnit(entity.getUnit());
        alert.setWindowStart(entity.getWindowStart());
        alert.setWindowEnd(entity.getWindowEnd());
        alert.setMessage(entity.getMessage());
        alert.setLatitude(entity.getLatitude());
        alert.setLongitude(entity.getLongitude());
        alert.setGeneratedAt(entity.getGeneratedAt());
        return alert;
    }

    /** Row to response body. Entities never leave the service layer. */
    public AlertDto toDto(AlertEntity entity) {
        return new AlertDto(
                entity.getAlertId(),
                entity.getStationId(),
                entity.getStationName(),
                entity.getDistrictId(),
                entity.getSensorType(),
                entity.getSeverity(),
                entity.getMetric(),
                entity.getObservedValue(),
                entity.getThresholdValue(),
                entity.getUnit(),
                entity.getWindowStart(),
                entity.getWindowEnd(),
                entity.getMessage(),
                entity.getLatitude(),
                entity.getLongitude(),
                entity.getGeneratedAt(),
                entity.getReceivedAt(),
                entity.isAcknowledged(),
                entity.getAcknowledgedAt(),
                entity.getAcknowledgedBy());
    }

    private static Instant firstNonNull(Instant... candidates) {
        for (Instant candidate : candidates) {
            if (candidate != null) {
                return candidate;
            }
        }
        return Instant.EPOCH;
    }
}
