package com.stream.processing.ingest.web;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.stream.processing.common.MeasurementUnit;
import com.stream.processing.common.SensorReading;
import com.stream.processing.common.SensorType;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

import java.time.Instant;
import java.util.UUID;

/**
 * The externally supplied form of a reading, accepted by {@code POST /api/v1/telemetry/readings}.
 *
 * <p>A separate type from {@link SensorReading} for two reasons. The event on the wire belongs to
 * common-model and must stay free of web concerns, so the validation constraints cannot live on
 * it; and a caller must not be able to set the fields the service owns - {@code ingestedAt} is
 * stamped by the publisher, and a caller-supplied value would corrupt the scrape-lag measurement.
 * Constraints here are stricter than {@link SensorReading#isValid()} on purpose: an HTTP client
 * gets a 400 telling it what is wrong, whereas a bad record arriving any other way is
 * dead-lettered for later triage.</p>
 *
 * @param readingId  optional; a client-supplied id makes a retried POST traceable, otherwise one
 *                   is generated
 * @param eventTime  optional; defaults to now, for clients that simply report "current" values
 * @param unit       optional; defaults to the unit implied by the sensor type
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SensorReadingRequest(
        String readingId,

        @NotBlank(message = "stationId is required")
        String stationId,

        String stationName,

        String districtId,

        @NotNull(message = "sensorType is required")
        SensorType sensorType,

        @NotNull(message = "value is required")
        @PositiveOrZero(message = "value must not be negative")
        Double value,

        MeasurementUnit unit,

        @DecimalMin(value = "-90.0", message = "latitude must be within [-90,90]")
        @DecimalMax(value = "90.0", message = "latitude must be within [-90,90]")
        double latitude,

        @DecimalMin(value = "-180.0", message = "longitude must be within [-180,180]")
        @DecimalMax(value = "180.0", message = "longitude must be within [-180,180]")
        double longitude,

        Instant eventTime,

        String source) {

    /** Marks readings that entered the pipeline over HTTP rather than from a scraper. */
    public static final String DEFAULT_SOURCE = "REST_API";

    /**
     * Maps onto the wire event, filling the optional fields. Station metadata that the caller
     * omitted is completed afterwards from the catalogue by
     * {@link com.stream.processing.ingest.station.StationRegistry#enrich(SensorReading)}.
     */
    public SensorReading toReading() {
        SensorReading reading = new SensorReading();
        reading.setReadingId(readingId == null || readingId.isBlank()
                ? UUID.randomUUID().toString() : readingId);
        reading.setStationId(stationId);
        reading.setStationName(stationName);
        reading.setDistrictId(districtId);
        reading.setSensorType(sensorType);
        reading.setValue(value == null ? Double.NaN : value);
        reading.setUnit(unit != null ? unit : (sensorType == null ? null : sensorType.getUnit()));
        reading.setLatitude(latitude);
        reading.setLongitude(longitude);
        reading.setEventTime(eventTime == null ? Instant.now() : eventTime);
        reading.setSource(source == null || source.isBlank() ? DEFAULT_SOURCE : source);
        return reading;
    }
}
