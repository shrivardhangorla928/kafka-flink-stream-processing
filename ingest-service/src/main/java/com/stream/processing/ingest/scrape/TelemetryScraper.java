package com.stream.processing.ingest.scrape;

import com.stream.processing.common.SensorReading;
import com.stream.processing.ingest.publish.TelemetryPublisher;
import com.stream.processing.ingest.simulator.ReadingSimulator;
import com.stream.processing.ingest.station.Station;
import com.stream.processing.ingest.station.StationRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * The scheduled scrape: once per interval it polls every enabled station and publishes a reading.
 *
 * <p>This stands in for the fleet of site-specific scrapers the real platform runs. Because the
 * whole bean is conditional on {@code stream.ingest.simulator.enabled}, the service can be
 * deployed as a pure REST ingest gateway - and tests that would otherwise race against a
 * background round can switch the schedule off entirely rather than trying to out-run it.</p>
 *
 * <p>The interval is read as a property placeholder on {@link Scheduled} because Spring resolves
 * the schedule before any bean is available to consult; the same key is bound on
 * {@code IngestProperties.scrapeIntervalMs} so it is still validated and documented in one
 * place.</p>
 */
@Component
@ConditionalOnProperty(prefix = "stream.ingest.simulator", name = "enabled", havingValue = "true",
        matchIfMissing = true)
public class TelemetryScraper {

    private static final Logger LOG = LoggerFactory.getLogger(TelemetryScraper.class);

    private final StationRegistry stationRegistry;
    private final ReadingSimulator simulator;
    private final TelemetryPublisher publisher;

    public TelemetryScraper(StationRegistry stationRegistry, ReadingSimulator simulator,
                            TelemetryPublisher publisher) {
        this.stationRegistry = stationRegistry;
        this.simulator = simulator;
        this.publisher = publisher;
    }

    /**
     * Fixed-rate rather than fixed-delay: the downstream 5-minute tumbling window assumes a
     * steady five readings per station, and a fixed delay would let a slow round stretch the
     * spacing and quietly change what a window sums to.
     */
    @Scheduled(fixedRateString = "${stream.ingest.scrape-interval-ms:60000}")
    public void scheduledScrape() {
        int published = scrape(0);
        LOG.debug("Scheduled scrape published {} readings", published);
    }

    /**
     * Runs one scrape round out of cycle, optionally forcing storms first, so a demonstration can
     * provoke alerts without waiting several minutes for the schedule.
     *
     * @param stormStations number of additional stations to push into a storm episode before the
     *                      round; zero leaves the storm state to the simulator
     * @return the number of readings handed to the producer
     */
    public int burst(int stormStations) {
        List<Station> stations = stationRegistry.enabled();
        if (stormStations > 0) {
            int started = simulator.forceStorms(stations, stormStations);
            LOG.info("Burst requested {} storm stations, started {}", stormStations, started);
        }
        return scrape(stormStations);
    }

    private int scrape(int requestedStorms) {
        List<Station> stations = stationRegistry.enabled();
        if (stations.isEmpty()) {
            LOG.warn("No enabled stations in the catalogue; nothing to scrape");
            return 0;
        }
        Instant observedAt = Instant.now();
        List<SensorReading> readings = simulator.simulateRound(stations, observedAt);
        int published = publisher.publishAll(readings);
        LOG.info("Scraped {} stations at {} ({} storm episodes active, {} forced)",
                published, observedAt, simulator.activeStormCount(), requestedStorms);
        return published;
    }
}
