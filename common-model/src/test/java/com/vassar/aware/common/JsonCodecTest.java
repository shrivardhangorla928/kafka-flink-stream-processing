package com.vassar.aware.common;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.UncheckedIOException;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The three services serialise and deserialise these events independently, so the wire format
 * is a contract rather than an implementation detail. These tests pin it.
 */
class JsonCodecTest {

    private static final Instant EVENT_TIME = Instant.parse("2026-08-11T10:15:00Z");

    @Test
    @DisplayName("instants are written as ISO-8601 strings, not epoch decimals")
    void writesInstantsAsIso8601() {
        SensorReading reading = reading();

        String json = JsonCodec.toJson(reading);

        assertThat(json).contains("\"eventTime\":\"2026-08-11T10:15:00Z\"");
        assertThat(json).doesNotContain("1786529700");
    }

    @Test
    @DisplayName("a reading round-trips through JSON unchanged")
    void roundTripsSensorReading() {
        SensorReading original = reading();

        SensorReading restored = JsonCodec.fromJson(JsonCodec.toJson(original), SensorReading.class);

        assertThat(restored).isEqualTo(original);
    }

    @Test
    @DisplayName("an aggregate round-trips through JSON unchanged")
    void roundTripsAggregate() {
        StationWindowAggregate original = new StationWindowAggregate();
        original.setStationId("STN-0007");
        original.setStationName("Vijayawada AWS");
        original.setDistrictId("AP-KRISHNA");
        original.setSensorType(SensorType.RAINFALL);
        original.setUnit(MeasurementUnit.MM);
        original.setWindowStart(EVENT_TIME);
        original.setWindowEnd(EVENT_TIME.plusSeconds(300));
        original.setReadingCount(5);
        original.setSum(62.5);
        original.setMin(10.0);
        original.setMax(15.0);
        original.setAvg(12.5);
        original.setAggregatedValue(62.5);
        original.setLatitude(16.5);
        original.setLongitude(80.6);
        original.setComputedAt(EVENT_TIME.plusSeconds(330));

        StationWindowAggregate restored =
                JsonCodec.fromJson(JsonCodec.toJson(original), StationWindowAggregate.class);

        assertThat(restored).isEqualTo(original);
        assertThat(restored.getComputedAt()).isEqualTo(original.getComputedAt());
        assertThat(restored.getStationName()).isEqualTo("Vijayawada AWS");
    }

    @Test
    @DisplayName("an alert round-trips through JSON unchanged")
    void roundTripsAlert() {
        Alert original = new Alert();
        original.setAlertId("de305d54-75b4-431b-adb2-eb6b9e546014");
        original.setStationId("STN-0007");
        original.setStationName("Vijayawada AWS");
        original.setDistrictId("AP-KRISHNA");
        original.setSensorType(SensorType.RAINFALL);
        original.setSeverity(Severity.SEVERE);
        original.setMetric("RAINFALL_WINDOW_SUM");
        original.setObservedValue(41.0);
        original.setThresholdValue(30.0);
        original.setUnit(MeasurementUnit.MM);
        original.setWindowStart(EVENT_TIME);
        original.setWindowEnd(EVENT_TIME.plusSeconds(300));
        original.setMessage("Rainfall at Vijayawada AWS reached 41.0 mm");
        original.setLatitude(16.5);
        original.setLongitude(80.6);
        original.setGeneratedAt(EVENT_TIME.plusSeconds(330));

        String json = JsonCodec.toJson(original);
        Alert restored = JsonCodec.fromJson(json, Alert.class);

        assertThat(restored).isEqualTo(original);
        assertThat(restored.getSeverity()).isEqualTo(Severity.SEVERE);
        assertThat(restored.getObservedValue()).isEqualTo(41.0);
        assertThat(json).contains("\"severity\":\"SEVERE\"");
    }

    @Test
    @DisplayName("unknown fields are tolerated so a producer can add one without breaking consumers")
    void ignoresUnknownProperties() {
        String json = """
                {"stationId":"STN-0001","sensorType":"RAINFALL","value":4.2,
                 "eventTime":"2026-08-11T10:15:00Z","someFieldAddedLater":"whatever"}
                """;

        SensorReading restored = JsonCodec.fromJson(json, SensorReading.class);

        assertThat(restored.getStationId()).isEqualTo("STN-0001");
        assertThat(restored.getValue()).isEqualTo(4.2);
    }

    @Test
    @DisplayName("bytes and string helpers agree")
    void bytesAndStringHelpersAgree() {
        SensorReading reading = reading();

        assertThat(JsonCodec.fromBytes(JsonCodec.toBytes(reading), SensorReading.class))
                .isEqualTo(JsonCodec.fromJson(JsonCodec.toJson(reading), SensorReading.class));
    }

    @Test
    @DisplayName("malformed payloads fail loudly rather than yielding a half-populated object")
    void rejectsMalformedJson() {
        assertThatThrownBy(() -> JsonCodec.fromJson("{\"stationId\":", SensorReading.class))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("SensorReading");
    }

    @Test
    @DisplayName("create() hands out independently configured mappers, not the shared singleton")
    void createReturnsFreshMapper() {
        assertThat(JsonCodec.create())
                .isNotSameAs(JsonCodec.mapper())
                .isNotSameAs(JsonCodec.create());
    }

    private static SensorReading reading() {
        SensorReading reading = new SensorReading();
        reading.setReadingId("11111111-2222-3333-4444-555555555555");
        reading.setStationId("STN-0007");
        reading.setStationName("Vijayawada AWS");
        reading.setDistrictId("AP-KRISHNA");
        reading.setSensorType(SensorType.RAINFALL);
        reading.setValue(12.5);
        reading.setUnit(MeasurementUnit.MM);
        reading.setLatitude(16.5062);
        reading.setLongitude(80.6480);
        reading.setEventTime(EVENT_TIME);
        reading.setIngestedAt(EVENT_TIME.plusSeconds(2));
        reading.setSource("IMD_SCRAPER");
        return reading;
    }
}
