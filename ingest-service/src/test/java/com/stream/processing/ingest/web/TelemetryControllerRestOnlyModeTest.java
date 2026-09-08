package com.stream.processing.ingest.web;

import com.stream.processing.ingest.config.IngestProperties;
import com.stream.processing.ingest.publish.TelemetryPublisher;
import com.stream.processing.ingest.scrape.TelemetryScraper;
import com.stream.processing.ingest.simulator.ReadingSimulator;
import com.stream.processing.ingest.station.StationRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The service deployed as a pure REST ingest gateway, with
 * {@code stream.ingest.simulator.enabled=false}.
 *
 * <p>In that mode the scraper bean does not exist at all, which is exactly the situation the
 * controller's {@link ObjectProvider} injection is there to survive. The slice is built with
 * MockMvc's standalone setup rather than {@code @WebMvcTest} because the point is to run the
 * controller with the scraper genuinely absent, which a mocked bean cannot reproduce.</p>
 */
class TelemetryControllerRestOnlyModeTest {

    private static final String BASE = "/api/v1/telemetry";

    private TelemetryPublisher publisher;
    private MockMvc mockMvc;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        publisher = mock(TelemetryPublisher.class);
        when(publisher.publish(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(publisher.publishAll(anyCollection()))
                .thenAnswer(invocation -> ((List<?>) invocation.getArgument(0)).size());

        ObjectProvider<TelemetryScraper> noScraper = mock(ObjectProvider.class);
        when(noScraper.getIfAvailable()).thenReturn(null);

        TelemetryController controller = new TelemetryController(publisher, mock(StationRegistry.class),
                mock(ReadingSimulator.class), new IngestProperties(), noScraper);
        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new ApiExceptionHandler())
                .build();
    }

    @Test
    @DisplayName("a burst answers 503 with a problem document when the simulator is switched off")
    void burstIsUnavailableWithoutTheSimulator() throws Exception {
        mockMvc.perform(post(BASE + "/simulate/burst").param("stormStations", "3"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.title").value("Simulator disabled"))
                .andExpect(jsonPath("$.status").value(503))
                .andExpect(jsonPath("$.detail").value(
                        "Burst requires stream.ingest.simulator.enabled=true; "
                                + "the service is in REST-only mode"))
                .andExpect(jsonPath("$.type").value("urn:stream:problem:simulator-disabled"));
    }

    @Test
    @DisplayName("an invalid burst request is still rejected before the simulator is consulted")
    void invalidBurstIsRejectedBeforeTheSimulatorCheck() throws Exception {
        mockMvc.perform(post(BASE + "/simulate/burst").param("stormStations", "-2"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value("stormStations must not be negative"));
    }

    @Test
    @DisplayName("pushed readings are still accepted in REST-only mode")
    void pushedReadingsStillWork() throws Exception {
        mockMvc.perform(post(BASE + "/readings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"stationId\":\"STN-0001\",\"sensorType\":\"RAINFALL\",\"value\":2.5}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.accepted").value(1));
    }
}
