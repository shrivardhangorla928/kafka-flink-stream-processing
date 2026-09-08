package com.vassar.aware.ingest.station;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.vassar.aware.common.SensorType;

/**
 * One entry of the simulated station catalogue.
 *
 * <p>A record rather than a POJO because the catalogue is immutable once loaded and is read
 * concurrently by the scheduled scraper and by REST threads; immutability removes the need for
 * any synchronisation. It is intentionally an ingest-local type and not part of common-model:
 * downstream stages receive the station metadata denormalised onto each reading and never need
 * the catalogue itself.</p>
 *
 * @param baseline the value the station reports while behaving normally, in the unit implied by
 *                 {@code sensorType}; the simulator jitters around it
 * @param enabled  whether the scraper polls the station, so a station can be parked in the
 *                 catalogue without emitting data
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Station(
        String stationId,
        String stationName,
        String districtId,
        SensorType sensorType,
        double latitude,
        double longitude,
        double baseline,
        boolean enabled) {
}
