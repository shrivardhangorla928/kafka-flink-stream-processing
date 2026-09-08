package com.vassar.aware.ingest.station;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vassar.aware.common.SensorReading;
import com.vassar.aware.ingest.config.IngestProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The station catalogue, loaded once from a bundled JSON resource.
 *
 * <p>In the real platform this list comes from a master-data service; bundling it as a classpath
 * resource keeps the dissertation's pipeline self-contained and reproducible, while the resource
 * location stays a property so a mounted ConfigMap can replace it in Kubernetes without a
 * rebuild.</p>
 *
 * <p>Loading eagerly in the constructor is deliberate: a missing or malformed catalogue should
 * fail the container's readiness probe at start-up, not the first scrape a minute later.</p>
 */
@Component
public class StationRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(StationRegistry.class);

    private final List<Station> stations;
    private final List<Station> enabledStations;
    private final Map<String, Station> byId;

    public StationRegistry(ResourceLoader resourceLoader, ObjectMapper objectMapper, IngestProperties properties) {
        this.stations = load(resourceLoader, objectMapper, properties.getStationsResource());
        this.enabledStations = this.stations.stream().filter(Station::enabled).toList();

        Map<String, Station> index = new LinkedHashMap<>();
        for (Station station : this.stations) {
            Station previous = index.put(station.stationId(), station);
            if (previous != null) {
                throw new IllegalStateException("Duplicate stationId in catalogue: " + station.stationId());
            }
        }
        this.byId = Collections.unmodifiableMap(index);

        LOG.info("Loaded {} stations ({} enabled) from {}", stations.size(), enabledStations.size(),
                properties.getStationsResource());
    }

    private static List<Station> load(ResourceLoader resourceLoader, ObjectMapper objectMapper, String location) {
        Resource resource = resourceLoader.getResource(location);
        if (!resource.exists()) {
            throw new IllegalStateException("Station catalogue not found at " + location);
        }
        try (InputStream in = resource.getInputStream()) {
            List<Station> loaded = objectMapper.readValue(in, new TypeReference<List<Station>>() { });
            if (loaded == null || loaded.isEmpty()) {
                throw new IllegalStateException("Station catalogue at " + location + " is empty");
            }
            return List.copyOf(loaded);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read station catalogue from " + location, e);
        }
    }

    /** Every station in the catalogue, enabled or not. */
    public List<Station> all() {
        return stations;
    }

    /** The stations the scraper polls. */
    public List<Station> enabled() {
        return enabledStations;
    }

    public Optional<Station> findById(String stationId) {
        return stationId == null ? Optional.empty() : Optional.ofNullable(byId.get(stationId));
    }

    /**
     * Fills in the metadata a caller left out of an externally supplied reading.
     *
     * <p>Existing values win, so an external system that genuinely knows better than the
     * catalogue is never overruled; this only stops a terse {@code {stationId, value}} payload
     * from reaching Flink without the district and coordinates the alert stage needs.</p>
     */
    public void enrich(SensorReading reading) {
        if (reading == null) {
            return;
        }
        findById(reading.getStationId()).ifPresent(station -> {
            if (isBlank(reading.getStationName())) {
                reading.setStationName(station.stationName());
            }
            if (isBlank(reading.getDistrictId())) {
                reading.setDistrictId(station.districtId());
            }
            if (reading.getSensorType() == null) {
                reading.setSensorType(station.sensorType());
            }
            if (reading.getUnit() == null && reading.getSensorType() != null) {
                reading.setUnit(reading.getSensorType().getUnit());
            }
            if (reading.getLatitude() == 0.0d && reading.getLongitude() == 0.0d) {
                reading.setLatitude(station.latitude());
                reading.setLongitude(station.longitude());
            }
        });
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
