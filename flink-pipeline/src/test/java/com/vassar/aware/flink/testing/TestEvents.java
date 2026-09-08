package com.vassar.aware.flink.testing;

import com.vassar.aware.common.JsonCodec;
import com.vassar.aware.common.SensorReading;
import com.vassar.aware.common.SensorType;
import com.vassar.aware.common.StationWindowAggregate;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/** Builders for the events the tests feed through the pipeline. */
public final class TestEvents {

    private TestEvents() {
        // static holder
    }

    public static SensorReading reading(String stationId, SensorType sensorType, double value, Instant eventTime) {
        SensorReading reading = new SensorReading();
        reading.setReadingId(stationId + '@' + eventTime);
        reading.setStationId(stationId);
        reading.setStationName(stationId + " Gauge");
        reading.setDistrictId("D-" + stationId);
        reading.setSensorType(sensorType);
        reading.setValue(value);
        reading.setUnit(sensorType.getUnit());
        reading.setLatitude(16.5);
        reading.setLongitude(80.6);
        reading.setEventTime(eventTime);
        reading.setIngestedAt(eventTime.plusSeconds(2));
        reading.setSource("TEST_SCRAPER");
        return reading;
    }

    /** A reading serialised exactly as the ingest service would put it on the topic. */
    public static byte[] readingBytes(String stationId, SensorType sensorType, double value, Instant eventTime) {
        return JsonCodec.toBytes(reading(stationId, sensorType, value, eventTime));
    }

    public static byte[] bytes(String payload) {
        return payload.getBytes(StandardCharsets.UTF_8);
    }

    public static StationWindowAggregate aggregate(String stationId,
                                                   SensorType sensorType,
                                                   double aggregatedValue,
                                                   Instant windowStart,
                                                   Instant windowEnd) {
        StationWindowAggregate aggregate = new StationWindowAggregate();
        aggregate.setStationId(stationId);
        aggregate.setStationName(stationId + " Gauge");
        aggregate.setDistrictId("D-" + stationId);
        aggregate.setSensorType(sensorType);
        aggregate.setUnit(sensorType.getUnit());
        aggregate.setWindowStart(windowStart);
        aggregate.setWindowEnd(windowEnd);
        aggregate.setReadingCount(3L);
        aggregate.setSum(aggregatedValue);
        aggregate.setMin(0.0d);
        aggregate.setMax(aggregatedValue);
        aggregate.setAvg(aggregatedValue / 3.0d);
        aggregate.setAggregatedValue(aggregatedValue);
        aggregate.setLatitude(16.5);
        aggregate.setLongitude(80.6);
        aggregate.setComputedAt(windowEnd.plusSeconds(1));
        return aggregate;
    }
}
