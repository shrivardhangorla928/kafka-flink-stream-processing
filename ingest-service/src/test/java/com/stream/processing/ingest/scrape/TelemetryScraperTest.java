package com.stream.processing.ingest.scrape;

import com.stream.processing.common.SensorReading;
import com.stream.processing.ingest.publish.TelemetryPublisher;
import com.stream.processing.ingest.simulator.ReadingSimulator;
import com.stream.processing.ingest.simulator.SimulatorSettings;
import com.stream.processing.ingest.station.Station;
import com.stream.processing.ingest.station.StationRegistry;
import com.stream.processing.ingest.support.StationFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * The scraper owns the "one reading per enabled station per cycle" rule that the downstream
 * 5-minute window's arithmetic depends on, so that is what these tests pin.
 */
class TelemetryScraperTest {

    private StationRegistry registry;
    private TelemetryPublisher publisher;
    private ReadingSimulator simulator;
    private TelemetryScraper scraper;

    @BeforeEach
    void setUp() {
        registry = mock(StationRegistry.class);
        publisher = mock(TelemetryPublisher.class);
        simulator = new ReadingSimulator(SimulatorSettings.defaults(), "STREAM_SIMULATOR");
        scraper = new TelemetryScraper(registry, simulator, publisher);
        when(publisher.publishAll(anyCollection()))
                .thenAnswer(invocation -> ((Collection<?>) invocation.getArgument(0)).size());
    }

    @SuppressWarnings("unchecked")
    private List<SensorReading> capturePublishedBatch() {
        ArgumentCaptor<Collection<SensorReading>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(publisher).publishAll(captor.capture());
        return List.copyOf(captor.getValue());
    }

    @Test
    @DisplayName("a scheduled round publishes exactly one reading per enabled station")
    void scheduledRoundPublishesOneReadingPerStation() {
        List<Station> stations = StationFixtures.catalogue(30, 12, 8);
        when(registry.enabled()).thenReturn(stations);

        scraper.scheduledScrape();

        List<SensorReading> published = capturePublishedBatch();
        assertThat(published).hasSize(50);
        assertThat(published).extracting(SensorReading::getStationId)
                .containsExactlyElementsOf(stations.stream().map(Station::stationId).toList());
        assertThat(published).allSatisfy(reading -> {
            assertThat(reading.isValid()).isTrue();
            assertThat(reading.getEventTime()).isBeforeOrEqualTo(Instant.now());
        });
    }

    @Test
    @DisplayName("parked stations are never scraped")
    void disabledStationsAreNotScraped() {
        when(registry.enabled()).thenReturn(List.of(StationFixtures.rainfall("STN-0001", 1.0d)));

        scraper.scheduledScrape();

        assertThat(capturePublishedBatch()).extracting(SensorReading::getStationId)
                .containsExactly("STN-0001");
    }

    @Test
    @DisplayName("an empty catalogue publishes nothing instead of failing the schedule")
    void emptyCatalogueIsANoOp() {
        when(registry.enabled()).thenReturn(List.of());

        assertThat(scraper.burst(0)).isZero();
        verifyNoInteractions(publisher);
    }

    @Test
    @DisplayName("a burst forces storms first, so the round it publishes can breach a threshold")
    void burstForcesStormsBeforeScraping() {
        List<Station> stations = StationFixtures.catalogue(30, 12, 8);
        when(registry.enabled()).thenReturn(stations);

        int published = scraper.burst(6);

        assertThat(published).isEqualTo(50);
        assertThat(simulator.activeStormCount()).isGreaterThanOrEqualTo(6);

        List<SensorReading> readings = capturePublishedBatch();
        long stormValues = readings.stream()
                .filter(reading -> simulator.isStorming(reading.getStationId()))
                .filter(reading -> reading.getValue() >= 4.0d)
                .count();
        assertThat(stormValues).isGreaterThanOrEqualTo(6L);
    }

    @Test
    @DisplayName("a burst with no forced storms still publishes a full round")
    void burstWithoutForcedStormsStillScrapes() {
        when(registry.enabled()).thenReturn(StationFixtures.catalogue(3, 1, 1));

        assertThat(scraper.burst(0)).isEqualTo(5);
        assertThat(capturePublishedBatch()).hasSize(5);
    }
}
