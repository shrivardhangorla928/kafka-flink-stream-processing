package com.stream.processing.alert.web.dto;

import com.stream.processing.common.MeasurementUnit;
import com.stream.processing.common.SensorType;
import com.stream.processing.common.Severity;

import java.time.Instant;

/**
 * Response representation of a stored alert.
 *
 * <p>A dedicated record rather than the entity: serialising a JPA entity ties the public API to
 * the schema, drags lazy proxies into Jackson and would leak {@code severityRank}, which is an
 * indexing detail nobody outside the database needs.</p>
 */
public record AlertDto(String alertId,
                       String stationId,
                       String stationName,
                       String districtId,
                       SensorType sensorType,
                       Severity severity,
                       String metric,
                       double observedValue,
                       double thresholdValue,
                       MeasurementUnit unit,
                       Instant windowStart,
                       Instant windowEnd,
                       String message,
                       double latitude,
                       double longitude,
                       Instant generatedAt,
                       Instant receivedAt,
                       boolean acknowledged,
                       Instant acknowledgedAt,
                       String acknowledgedBy) {
}
