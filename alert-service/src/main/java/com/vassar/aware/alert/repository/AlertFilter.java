package com.vassar.aware.alert.repository;

import com.vassar.aware.common.SensorType;
import com.vassar.aware.common.Severity;

import java.time.Instant;

/**
 * The optional criteria the listing endpoint accepts, gathered into one value so the controller,
 * the service and the {@link AlertSpecifications} builder all agree on their meaning.
 *
 * <p>Every field is nullable and means "do not constrain on this" when absent.</p>
 *
 * @param districtId  exact district match
 * @param stationId   exact station match
 * @param sensorType  exact sensor-type match
 * @param minSeverity <em>minimum</em> severity, not an exact match: asking for {@code WARNING}
 *                    must also return {@code SEVERE} and {@code EXTREME}, because an operator
 *                    filtering for warnings is asking "what is at least this bad"
 * @param from        inclusive lower bound on {@code generatedAt}
 * @param to          exclusive upper bound on {@code generatedAt}
 * @param acknowledged acknowledgement state
 */
public record AlertFilter(String districtId,
                          String stationId,
                          SensorType sensorType,
                          Severity minSeverity,
                          Instant from,
                          Instant to,
                          Boolean acknowledged) {

    /** A filter that constrains nothing. */
    public static AlertFilter none() {
        return new AlertFilter(null, null, null, null, null, null, null);
    }
}
