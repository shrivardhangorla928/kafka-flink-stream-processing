package com.stream.processing.ingest.scrape;

import com.stream.processing.common.SensorReading;
import com.stream.processing.common.SensorType;
import com.stream.processing.ingest.publish.TelemetryPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.OptionalDouble;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * The scraper owns the "publish only new, well-formed Desweather readings" rule: it must skip
 * stations whose {@code ldate} cannot be parsed and stations {@link StationRainState} reports as
 * unchanged, and it must turn the raw feed's cumulative rain counter into the per-cycle increment
 * the downstream 5-minute window sums.
 */
class TelemetryScraperTest {

    private static final DateTimeFormatter LDATE_FORMAT =
            DateTimeFormatter.ofPattern("dd/MM/yyyy hh:mm:ss a", Locale.ENGLISH);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private DesweatherClient desweatherClient;
    private StationRainState stationRainState;
    private TelemetryPublisher publisher;
    private TelemetryScraper scraper;

    @BeforeEach
    void setUp() {
        desweatherClient = mock(DesweatherClient.class);
        stationRainState = mock(StationRainState.class);
        publisher = mock(TelemetryPublisher.class);
        scraper = new TelemetryScraper(desweatherClient, stationRainState, publisher);
    }

    @SuppressWarnings("unchecked")
    private List<SensorReading> capturePublishedBatch() {
        ArgumentCaptor<Collection<SensorReading>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(publisher).publishAll(captor.capture());
        return List.copyOf(captor.getValue());
    }

    private static DesweatherReading station(long clientId, String ldate, double rain) {
        DesweatherReading r = new DesweatherReading();
        r.setClientid(clientId);
        r.setLocation("Station " + clientId);
        r.setDistrict("DIST-" + clientId);
        r.setLatitude(16.5d);
        r.setLongitude(80.6d);
        r.setRain(rain);
        r.setLdate(ldate);
        return r;
    }

    @Test
    @DisplayName("an empty feed publishes nothing instead of failing the schedule")
    void emptyFeedIsANoOp() {
        when(desweatherClient.fetch()).thenReturn(List.of());

        scraper.scheduledScrape();

        verifyNoInteractions(publisher);
    }

    @Test
    @DisplayName("a new station is published with the raw rain value as its increment")
    void newStationPublishesRawRainAsIncrement() {
        DesweatherReading reading = station(1001L, "25/09/2026 10:00:00 AM", 4.5d);
        when(desweatherClient.fetch()).thenReturn(List.of(reading));
        when(stationRainState.incrementIfChanged(eq(1001L), any(Instant.class), eq(4.5d)))
                .thenReturn(OptionalDouble.of(4.5d));

        scraper.scheduledScrape();

        List<SensorReading> published = capturePublishedBatch();
        assertThat(published).hasSize(1);
        SensorReading out = published.get(0);
        assertThat(out.getStationId()).isEqualTo("1001");
        assertThat(out.getStationName()).isEqualTo("Station 1001");
        assertThat(out.getDistrictId()).isEqualTo("DIST-1001");
        assertThat(out.getSensorType()).isEqualTo(SensorType.RAINFALL);
        assertThat(out.getValue()).isEqualTo(4.5d);
        assertThat(out.getUnit()).isEqualTo(SensorType.RAINFALL.getUnit());
        assertThat(out.getLatitude()).isEqualTo(16.5d);
        assertThat(out.getLongitude()).isEqualTo(80.6d);
        assertThat(out.getSource()).isEqualTo("DESWEATHER_AP");
        assertThat(out.getEventTime()).isEqualTo(
                java.time.LocalDateTime.parse("25/09/2026 10:00:00 AM", LDATE_FORMAT).atZone(IST).toInstant());
        assertThat(out.isValid()).isTrue();
    }

    @Test
    @DisplayName("a station with an unchanged ldate is skipped")
    void unchangedStationIsSkipped() {
        DesweatherReading reading = station(1002L, "25/09/2026 10:00:00 AM", 4.5d);
        when(desweatherClient.fetch()).thenReturn(List.of(reading));
        when(stationRainState.incrementIfChanged(eq(1002L), any(Instant.class), anyDouble()))
                .thenReturn(OptionalDouble.empty());

        scraper.scheduledScrape();

        List<SensorReading> published = capturePublishedBatch();
        assertThat(published).isEmpty();
    }

    @Test
    @DisplayName("a station with an unparseable ldate is skipped without consulting rain state")
    void unparseableLdateIsSkipped() {
        DesweatherReading reading = station(1003L, "not-a-date", 4.5d);
        when(desweatherClient.fetch()).thenReturn(List.of(reading));

        scraper.scheduledScrape();

        List<SensorReading> published = capturePublishedBatch();
        assertThat(published).isEmpty();
        verifyNoInteractionsWithRainState();
    }

    private void verifyNoInteractionsWithRainState() {
        verifyNoInteractions(stationRainState);
    }

    @Test
    @DisplayName("only stations reporting new data are published out of a mixed round")
    void mixedRoundPublishesOnlyNewData() {
        DesweatherReading changed = station(2001L, "25/09/2026 10:05:00 AM", 8.0d);
        DesweatherReading unchanged = station(2002L, "25/09/2026 10:00:00 AM", 3.0d);
        DesweatherReading badDate = station(2003L, "garbage", 1.0d);
        when(desweatherClient.fetch()).thenReturn(List.of(changed, unchanged, badDate));
        when(stationRainState.incrementIfChanged(eq(2001L), any(Instant.class), eq(8.0d)))
                .thenReturn(OptionalDouble.of(3.5d));
        when(stationRainState.incrementIfChanged(eq(2002L), any(Instant.class), eq(3.0d)))
                .thenReturn(OptionalDouble.empty());

        scraper.scheduledScrape();

        List<SensorReading> published = capturePublishedBatch();
        assertThat(published).hasSize(1);
        assertThat(published.get(0).getStationId()).isEqualTo("2001");
        assertThat(published.get(0).getValue()).isEqualTo(3.5d);
    }
}
