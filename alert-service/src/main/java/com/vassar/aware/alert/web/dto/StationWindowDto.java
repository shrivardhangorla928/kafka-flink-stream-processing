package com.vassar.aware.alert.web.dto;

import com.vassar.aware.common.MeasurementUnit;
import com.vassar.aware.common.SensorType;

import java.time.Instant;

/** Response representation of one windowed aggregate on a station's timeline. */
public record StationWindowDto(String stationId,
                               String stationName,
                               String districtId,
                               SensorType sensorType,
                               MeasurementUnit unit,
                               Instant windowStart,
                               Instant windowEnd,
                               long readingCount,
                               double sum,
                               double min,
                               double max,
                               double avg,
                               double aggregatedValue,
                               Instant computedAt,
                               Instant receivedAt) {
}
