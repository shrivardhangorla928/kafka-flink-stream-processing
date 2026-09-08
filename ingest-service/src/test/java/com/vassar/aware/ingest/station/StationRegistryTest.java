package com.vassar.aware.ingest.station;

import com.vassar.aware.common.JsonCodec;
import com.vassar.aware.common.MeasurementUnit;
import com.vassar.aware.common.SensorReading;
import com.vassar.aware.common.SensorType;
import com.vassar.aware.ingest.config.IngestProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.DefaultResourceLoader;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Guards the bundled catalogue as much as the loader: the simulated fleet's composition and its
 * baselines are what make the downstream demonstration meaningful, so both are asserted here
 * rather than only being described in prose.
 */
class StationRegistryTest {

    private static final int READINGS_PER_WINDOW = 5;

    private static StationRegistry registryFor(String location) {
        IngestProperties properties = new IngestProperties();
        properties.setStationsResource(location);
        return new StationRegistry(new DefaultResourceLoader(), JsonCodec.create(), properties);
    }

    private static StationRegistry bundledRegistry() {
        return registryFor(new IngestProperties().getStationsResource());
    }

    @Test
    @DisplayName("the bundled catalogue holds 50 stations with the agreed sensor-type split")
    void loadsFiftyStationsWithTheExpectedSplit() {
        StationRegistry registry = bundledRegistry();

        assertThat(registry.all()).hasSize(50);
        assertThat(registry.all()).filteredOn(s -> s.sensorType() == SensorType.RAINFALL).hasSize(30);
        assertThat(registry.all()).filteredOn(s -> s.sensorType() == SensorType.RESERVOIR_LEVEL).hasSize(12);
        assertThat(registry.all()).filteredOn(s -> s.sensorType() == SensorType.RIVER_LEVEL).hasSize(8);
        assertThat(registry.enabled()).hasSize(50);
    }

    @Test
    @DisplayName("station ids are unique and follow the STN-nnnn convention")
    void stationIdsAreUniqueAndWellFormed() {
        StationRegistry registry = bundledRegistry();

        assertThat(registry.all()).extracting(Station::stationId)
                .doesNotHaveDuplicates()
                .allMatch(id -> id.matches("STN-\\d{4}"));
    }

    @Test
    @DisplayName("every station sits inside Andhra Pradesh and names a district")
    void stationsAreGeographicallyPlausible() {
        assertThat(bundledRegistry().all()).allSatisfy(station -> {
            assertThat(station.latitude()).isBetween(12.5d, 19.5d);
            assertThat(station.longitude()).isBetween(77.0d, 85.0d);
            assertThat(station.districtId()).startsWith("AP-");
            assertThat(station.stationName()).isNotBlank();
        });
    }

    @Test
    @DisplayName("baselines keep a normally behaving station clear of its downstream threshold")
    void baselinesStayBelowTheAlertingBands() {
        assertThat(bundledRegistry().all()).allSatisfy(station -> {
            assertThat(station.baseline()).isPositive();
            switch (station.sensorType()) {
                // the downstream window sums rainfall, so it is the window total that must clear 15 mm
                case RAINFALL -> assertThat(station.baseline() * READINGS_PER_WINDOW).isLessThan(15.0d);
                case RESERVOIR_LEVEL -> assertThat(station.baseline()).isLessThan(85.0d);
                case RIVER_LEVEL -> assertThat(station.baseline()).isLessThan(8.0d);
            }
        });
    }

    @Test
    @DisplayName("disabled stations are excluded from the scrape list but stay in the catalogue")
    void disabledStationsAreNotScraped() {
        StationRegistry registry = registryFor("classpath:stations-mixed-enablement.json");

        assertThat(registry.all()).hasSize(2);
        assertThat(registry.enabled()).extracting(Station::stationId).containsExactly("STN-0001");
    }

    @Test
    @DisplayName("lookup by id finds known stations and is empty for anything else")
    void findsStationsById() {
        StationRegistry registry = bundledRegistry();

        assertThat(registry.findById("STN-0001")).isPresent();
        assertThat(registry.findById("STN-9999")).isEmpty();
        assertThat(registry.findById(null)).isEmpty();
    }

    @Test
    @DisplayName("enrichment completes a terse reading from the catalogue")
    void enrichmentFillsMissingMetadata() {
        StationRegistry registry = bundledRegistry();
        Station station = registry.all().get(0);

        SensorReading reading = new SensorReading();
        reading.setStationId(station.stationId());
        reading.setValue(3.0d);

        registry.enrich(reading);

        assertThat(reading.getStationName()).isEqualTo(station.stationName());
        assertThat(reading.getDistrictId()).isEqualTo(station.districtId());
        assertThat(reading.getSensorType()).isEqualTo(station.sensorType());
        assertThat(reading.getUnit()).isEqualTo(station.sensorType().getUnit());
        assertThat(reading.getLatitude()).isEqualTo(station.latitude());
        assertThat(reading.getLongitude()).isEqualTo(station.longitude());
    }

    @Test
    @DisplayName("enrichment never overrules values the caller supplied")
    void enrichmentKeepsCallerSuppliedValues() {
        StationRegistry registry = bundledRegistry();
        Station station = registry.all().get(0);

        SensorReading reading = new SensorReading();
        reading.setStationId(station.stationId());
        reading.setStationName("Field Survey Gauge");
        reading.setDistrictId("AP-OVERRIDE");
        reading.setSensorType(SensorType.RIVER_LEVEL);
        reading.setUnit(MeasurementUnit.METRE);
        reading.setLatitude(14.0d);
        reading.setLongitude(79.0d);

        registry.enrich(reading);

        assertThat(reading.getStationName()).isEqualTo("Field Survey Gauge");
        assertThat(reading.getDistrictId()).isEqualTo("AP-OVERRIDE");
        assertThat(reading.getSensorType()).isEqualTo(SensorType.RIVER_LEVEL);
        assertThat(reading.getLatitude()).isEqualTo(14.0d);
        assertThat(reading.getLongitude()).isEqualTo(79.0d);
    }

    @Test
    @DisplayName("enrichment tolerates nulls and unknown stations")
    void enrichmentIsSafeForUnknownStations() {
        StationRegistry registry = bundledRegistry();
        SensorReading unknown = new SensorReading();
        unknown.setStationId("STN-9999");

        registry.enrich(null);
        registry.enrich(unknown);

        assertThat(unknown.getStationName()).isNull();
    }

    @Test
    @DisplayName("a missing, empty or inconsistent catalogue fails the service at start-up")
    void badCataloguesFailFast() {
        assertThatThrownBy(() -> registryFor("classpath:does-not-exist.json"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not found");

        assertThatThrownBy(() -> registryFor("classpath:stations-empty.json"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("empty");

        assertThatThrownBy(() -> registryFor("classpath:stations-duplicate.json"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate stationId");
    }

    @Test
    @DisplayName("the catalogue is exposed as an unmodifiable snapshot")
    void catalogueIsImmutable() {
        StationRegistry registry = bundledRegistry();
        Optional<Station> first = registry.findById("STN-0001");

        assertThat(first).isPresent();
        assertThatThrownBy(() -> registry.all().add(first.orElseThrow()))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
