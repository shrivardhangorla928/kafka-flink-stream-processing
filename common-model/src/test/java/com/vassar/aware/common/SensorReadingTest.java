package com.vassar.aware.common;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code isValid()} is the gate that decides whether a record enters the pipeline or is
 * dead-lettered, so every rejection reason is pinned here.
 */
class SensorReadingTest {

    @Test
    @DisplayName("a fully populated reading is accepted")
    void acceptsCompleteReading() {
        assertThat(valid().isValid()).isTrue();
    }

    @Test
    @DisplayName("a reading of exactly zero is valid - no rain is a measurement, not a gap")
    void acceptsZero() {
        SensorReading reading = valid();
        reading.setValue(0.0);

        assertThat(reading.isValid()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   "})
    @DisplayName("a blank station id is rejected because the record cannot be keyed")
    void rejectsBlankStationId(String stationId) {
        SensorReading reading = valid();
        reading.setStationId(stationId);

        assertThat(reading.isValid()).isFalse();
    }

    @Test
    @DisplayName("a null station id is rejected")
    void rejectsNullStationId() {
        SensorReading reading = valid();
        reading.setStationId(null);

        assertThat(reading.isValid()).isFalse();
    }

    @Test
    @DisplayName("a missing sensor type is rejected because no threshold set applies")
    void rejectsMissingSensorType() {
        SensorReading reading = valid();
        reading.setSensorType(null);

        assertThat(reading.isValid()).isFalse();
    }

    @Test
    @DisplayName("a missing event time is rejected because the record cannot be windowed")
    void rejectsMissingEventTime() {
        SensorReading reading = valid();
        reading.setEventTime(null);

        assertThat(reading.isValid()).isFalse();
    }

    @Test
    @DisplayName("NaN, infinity and negative values are rejected before they poison an aggregate")
    void rejectsUnusableValues() {
        SensorReading nan = valid();
        nan.setValue(Double.NaN);

        SensorReading infinite = valid();
        infinite.setValue(Double.POSITIVE_INFINITY);

        SensorReading negative = valid();
        negative.setValue(-0.1);

        assertThat(nan.isValid()).isFalse();
        assertThat(infinite.isValid()).isFalse();
        assertThat(negative.isValid()).isFalse();
    }

    @Test
    @DisplayName("equality covers the whole record, and toString names the station")
    void equalityAndToString() {
        SensorReading a = valid();
        SensorReading b = valid();

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isEqualTo(a);
        assertThat(a).isNotEqualTo(null).isNotEqualTo("not a reading");

        b.setValue(99.9);
        assertThat(a).isNotEqualTo(b);

        assertThat(a.toString()).contains("STN-0007").contains("RAINFALL");
    }

    private static SensorReading valid() {
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
        reading.setEventTime(Instant.parse("2026-08-11T10:15:00Z"));
        reading.setIngestedAt(Instant.parse("2026-08-11T10:15:02Z"));
        reading.setSource("IMD_SCRAPER");
        return reading;
    }
}
