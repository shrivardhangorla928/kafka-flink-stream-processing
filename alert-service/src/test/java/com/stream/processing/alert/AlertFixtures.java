package com.stream.processing.alert;

import com.stream.processing.common.Alert;
import com.stream.processing.common.AlertIds;
import com.stream.processing.common.MeasurementUnit;
import com.stream.processing.common.SensorType;
import com.stream.processing.common.Severity;
import com.stream.processing.common.StationWindowAggregate;

import java.time.Duration;
import java.time.Instant;

/**
 * Builders for the events under test.
 *
 * <p>Alerts are built with the real {@link AlertIds#deterministicId} rather than a random id, so
 * every idempotency test is exercising the same id derivation the Flink job uses. A test that
 * invented its own duplicate ids could pass while the real replay path was broken.</p>
 */
public final class AlertFixtures {

    /** Fixed clock anchor, so window arithmetic in assertions is readable and reproducible. */
    public static final Instant WINDOW_START = Instant.parse("2026-08-11T06:00:00Z");
    public static final Duration WINDOW_SIZE = Duration.ofMinutes(5);

    private AlertFixtures() {
        // static holder
    }

    public static Alert alert(String stationId, SensorType sensorType, Severity severity) {
        return alert(stationId, sensorType, severity, WINDOW_START, "DIST-01");
    }

    public static Alert alert(String stationId,
                              SensorType sensorType,
                              Severity severity,
                              Instant windowStart,
                              String districtId) {
        Instant windowEnd = windowStart.plus(WINDOW_SIZE);

        Alert alert = new Alert();
        alert.setAlertId(AlertIds.deterministicId(stationId, sensorType, windowStart, windowEnd, severity));
        alert.setStationId(stationId);
        alert.setStationName("Station " + stationId);
        alert.setDistrictId(districtId);
        alert.setSensorType(sensorType);
        alert.setSeverity(severity);
        alert.setMetric(sensorType.name() + "_WINDOW_" + sensorType.getAggregationKind());
        alert.setObservedValue(87.5d);
        alert.setThresholdValue(60.0d);
        alert.setUnit(sensorType.getUnit());
        alert.setWindowStart(windowStart);
        alert.setWindowEnd(windowEnd);
        alert.setMessage(severity + " at " + stationId);
        alert.setLatitude(16.5062d);
        alert.setLongitude(80.6480d);
        alert.setGeneratedAt(windowEnd.plusSeconds(2));
        return alert;
    }

    public static StationWindowAggregate aggregate(String stationId,
                                                   SensorType sensorType,
                                                   Instant windowStart,
                                                   double aggregatedValue) {
        Instant windowEnd = windowStart.plus(WINDOW_SIZE);

        StationWindowAggregate aggregate = new StationWindowAggregate();
        aggregate.setStationId(stationId);
        aggregate.setStationName("Station " + stationId);
        aggregate.setDistrictId("DIST-01");
        aggregate.setSensorType(sensorType);
        aggregate.setUnit(sensorType.getUnit());
        aggregate.setWindowStart(windowStart);
        aggregate.setWindowEnd(windowEnd);
        aggregate.setReadingCount(12L);
        aggregate.setSum(aggregatedValue);
        aggregate.setMin(0.5d);
        aggregate.setMax(aggregatedValue);
        aggregate.setAvg(aggregatedValue / 12.0d);
        aggregate.setAggregatedValue(aggregatedValue);
        aggregate.setLatitude(16.5062d);
        aggregate.setLongitude(80.6480d);
        aggregate.setComputedAt(windowEnd.plusSeconds(1));
        return aggregate;
    }
}
