package com.stream.processing.ingest.web;

import com.stream.processing.common.SensorReading;
import com.stream.processing.common.Topics;
import com.stream.processing.ingest.config.IngestProperties;
import com.stream.processing.ingest.publish.TelemetryPublisher;
import com.stream.processing.ingest.scrape.TelemetryScraper;
import com.stream.processing.ingest.simulator.ReadingSimulator;
import com.stream.processing.ingest.station.Station;
import com.stream.processing.ingest.station.StationRegistry;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * HTTP face of the ingest service.
 *
 * <p>It exists for two reasons beyond convenience. Real deployments have upstream systems that
 * push rather than wait to be scraped, and those readings must enter through the same validation,
 * keying and metrics path as scraped ones - hence {@code POST /readings}. And a live
 * demonstration cannot wait five minutes for a tumbling window to fill from the schedule, so
 * {@code POST /simulate/burst} forces a round on demand.</p>
 *
 * <p>Writes answer 202, not 201: nothing is created at a retrievable URL, the reading has merely
 * been accepted for asynchronous publication.</p>
 */
@RestController
@RequestMapping(path = "/api/v1/telemetry", produces = MediaType.APPLICATION_JSON_VALUE)
public class TelemetryController {

    private static final Logger LOG = LoggerFactory.getLogger(TelemetryController.class);

    private final TelemetryPublisher publisher;
    private final StationRegistry stationRegistry;
    private final ReadingSimulator simulator;
    private final IngestProperties properties;

    /**
     * The scraper is injected lazily because it is conditional on the simulator being enabled;
     * an {@link ObjectProvider} lets this controller stay loadable in REST-only mode and answer
     * 503 for the one endpoint that needs it.
     */
    private final ObjectProvider<TelemetryScraper> scraperProvider;

    public TelemetryController(TelemetryPublisher publisher,
                              StationRegistry stationRegistry,
                              ReadingSimulator simulator,
                              IngestProperties properties,
                              ObjectProvider<TelemetryScraper> scraperProvider) {
        this.publisher = publisher;
        this.stationRegistry = stationRegistry;
        this.simulator = simulator;
        this.properties = properties;
        this.scraperProvider = scraperProvider;
    }

    @PostMapping(path = "/readings", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PublishReceipt ingest(@Valid @RequestBody SensorReadingRequest request) {
        publisher.publish(toEnrichedReading(request));
        return PublishReceipt.of(1, Topics.RAW_TELEMETRY);
    }

    @PostMapping(path = "/readings/batch", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PublishReceipt ingestBatch(@RequestBody List<@Valid SensorReadingRequest> requests) {
        if (requests == null || requests.isEmpty()) {
            throw new IllegalArgumentException("Batch must contain at least one reading");
        }
        if (requests.size() > properties.getMaxBatchSize()) {
            throw new IllegalArgumentException("Batch of " + requests.size()
                    + " exceeds stream.ingest.max-batch-size=" + properties.getMaxBatchSize());
        }
        List<SensorReading> readings = new ArrayList<>(requests.size());
        for (SensorReadingRequest request : requests) {
            readings.add(toEnrichedReading(request));
        }
        int accepted = publisher.publishAll(readings);
        return PublishReceipt.of(accepted, Topics.RAW_TELEMETRY);
    }

    /**
     * Forces an out-of-cycle scrape round.
     *
     * @param stormStations stations to push into a storm episode first, so the round is
     *                      guaranteed to contain threshold-breaching values
     */
    @PostMapping("/simulate/burst")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public BurstReceipt burst(@RequestParam(name = "stormStations", defaultValue = "0") int stormStations) {
        if (stormStations < 0) {
            throw new IllegalArgumentException("stormStations must not be negative");
        }
        int capped = properties.getMaxBurstStormStations();
        if (stormStations > capped) {
            throw new IllegalArgumentException("stormStations must not exceed "
                    + "stream.ingest.max-burst-storm-stations=" + capped);
        }
        TelemetryScraper scraper = scraperProvider.getIfAvailable();
        if (scraper == null) {
            throw new SimulatorDisabledException(
                    "Burst requires stream.ingest.simulator.enabled=true; the service is in REST-only mode");
        }
        int published = scraper.burst(stormStations);
        LOG.info("Burst published {} readings with {} requested storm stations", published, stormStations);
        return new BurstReceipt(published, stormStations, simulator.activeStormCount(), Instant.now());
    }

    @GetMapping("/stations")
    public List<Station> stations() {
        return stationRegistry.all();
    }

    private SensorReading toEnrichedReading(SensorReadingRequest request) {
        SensorReading reading = request.toReading();
        stationRegistry.enrich(reading);
        return reading;
    }
}
