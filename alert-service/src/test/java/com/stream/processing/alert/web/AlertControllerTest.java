package com.stream.processing.alert.web;

import com.stream.processing.alert.repository.AlertFilter;
import com.stream.processing.alert.service.AlertQueryService;
import com.stream.processing.alert.web.dto.AlertDto;
import com.stream.processing.alert.web.dto.AlertSummaryDto;
import com.stream.processing.alert.web.dto.PageResponse;
import com.stream.processing.common.MeasurementUnit;
import com.stream.processing.common.SensorType;
import com.stream.processing.common.Severity;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Web-layer tests for request binding and for the RFC 7807 error bodies.
 *
 * <p>The service is mocked here on purpose: this class is about what the HTTP layer does with a
 * request and with an exception. The query semantics themselves - minimum severity, filter
 * composition, the page cap - are proved end to end against a real database in
 * {@link AlertApiIntegrationTest}, because mocking the service would let them pass while the
 * translation into SQL was wrong.</p>
 */
@WebMvcTest(AlertController.class)
class AlertControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private AlertQueryService queryService;

    @Test
    void everyQueryParameterReachesTheFilter() throws Exception {
        when(queryService.search(any(), any())).thenReturn(emptyPage());

        mockMvc.perform(get("/api/v1/alerts")
                        .param("districtId", "DIST-02")
                        .param("stationId", "STN-7")
                        .param("sensorType", "RIVER_LEVEL")
                        .param("severity", "SEVERE")
                        .param("from", "2026-08-11T00:00:00Z")
                        .param("to", "2026-08-12T00:00:00Z")
                        .param("acknowledged", "false"))
                .andExpect(status().isOk());

        ArgumentCaptor<AlertFilter> captor = ArgumentCaptor.forClass(AlertFilter.class);
        verify(queryService).search(captor.capture(), any(Pageable.class));

        AlertFilter filter = captor.getValue();
        assertThat(filter.districtId()).isEqualTo("DIST-02");
        assertThat(filter.stationId()).isEqualTo("STN-7");
        assertThat(filter.sensorType()).isEqualTo(SensorType.RIVER_LEVEL);
        assertThat(filter.minSeverity()).isEqualTo(Severity.SEVERE);
        assertThat(filter.from()).isEqualTo(Instant.parse("2026-08-11T00:00:00Z"));
        assertThat(filter.to()).isEqualTo(Instant.parse("2026-08-12T00:00:00Z"));
        assertThat(filter.acknowledged()).isFalse();
    }

    @Test
    void anUnfilteredRequestConstrainsNothing() throws Exception {
        when(queryService.search(any(), any())).thenReturn(emptyPage());

        mockMvc.perform(get("/api/v1/alerts")).andExpect(status().isOk());

        ArgumentCaptor<AlertFilter> captor = ArgumentCaptor.forClass(AlertFilter.class);
        verify(queryService).search(captor.capture(), any(Pageable.class));
        assertThat(captor.getValue()).isEqualTo(AlertFilter.none());
    }

    @Test
    void theResponseEnvelopeIsTheStableFiveFieldShape() throws Exception {
        when(queryService.search(any(), any()))
                .thenReturn(new PageResponse<>(List.of(dto("a")), 2, 25, 51L, 3));

        mockMvc.perform(get("/api/v1/alerts"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].alertId").value("a"))
                .andExpect(jsonPath("$.page").value(2))
                .andExpect(jsonPath("$.size").value(25))
                .andExpect(jsonPath("$.totalElements").value(51))
                .andExpect(jsonPath("$.totalPages").value(3))
                // The indexing detail must never appear in the API.
                .andExpect(jsonPath("$.content[0].severityRank").doesNotExist());
    }

    @Test
    void anUnknownSeverityBandIsRejectedAsABadRequest() throws Exception {
        mockMvc.perform(get("/api/v1/alerts").param("severity", "CATASTROPHIC"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.title").value("Bad request"))
                .andExpect(jsonPath("$.detail").value(org.hamcrest.Matchers.containsString("severity")));
    }

    @Test
    void aMalformedInstantIsRejectedAsABadRequest() throws Exception {
        mockMvc.perform(get("/api/v1/alerts").param("from", "yesterday"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));
    }

    @Test
    void aKnownAlertIsReturned() throws Exception {
        when(queryService.findById("abc")).thenReturn(dto("abc"));

        mockMvc.perform(get("/api/v1/alerts/{id}", "abc"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.alertId").value("abc"))
                .andExpect(jsonPath("$.severity").value("SEVERE"));
    }

    @Test
    void anUnknownAlertIsAProblemDetailNotFound() throws Exception {
        when(queryService.findById("missing")).thenThrow(new AlertNotFoundException("missing"));

        mockMvc.perform(get("/api/v1/alerts/{id}", "missing"))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.title").value("Alert not found"))
                .andExpect(jsonPath("$.type").value("urn:stream:alert:not-found"))
                // Extension properties are flattened to the top level, as problem+json requires.
                .andExpect(jsonPath("$.alertId").value("missing"));
    }

    @Test
    void acknowledgingAnUnknownAlertIsAlsoANotFound() throws Exception {
        when(queryService.acknowledge(eq("missing"), anyString(), any()))
                .thenThrow(new AlertNotFoundException("missing"));

        mockMvc.perform(post("/api/v1/alerts/{id}/acknowledge", "missing")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"acknowledgedBy\":\"ops\"}"))
                .andExpect(status().isNotFound());
    }

    @Test
    void anAcknowledgementWithoutANameIsRejected() throws Exception {
        mockMvc.perform(post("/api/v1/alerts/{id}/acknowledge", "abc")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"acknowledgedBy\":\"   \"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Validation failed"))
                .andExpect(jsonPath("$.errors.acknowledgedBy").exists());
    }

    @Test
    void anAcknowledgementWithNoBodyAtAllIsRejected() throws Exception {
        mockMvc.perform(post("/api/v1/alerts/{id}/acknowledge", "abc")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Bad request"));
    }

    @Test
    void summaryDefaultsToTheLastTwentyFourHoursWhenNoRangeIsGiven() throws Exception {
        when(queryService.summary(any(), any())).thenReturn(
                new AlertSummaryDto(Instant.EPOCH, Instant.EPOCH, 0L, Map.of(), List.of()));

        mockMvc.perform(get("/api/v1/alerts/summary")).andExpect(status().isOk());

        ArgumentCaptor<Instant> from = ArgumentCaptor.forClass(Instant.class);
        ArgumentCaptor<Instant> to = ArgumentCaptor.forClass(Instant.class);
        verify(queryService).summary(from.capture(), to.capture());
        assertThat(java.time.Duration.between(from.getValue(), to.getValue()))
                .isEqualTo(java.time.Duration.ofHours(24));
    }

    @Test
    void anInvertedSummaryRangeIsRejected() throws Exception {
        mockMvc.perform(get("/api/v1/alerts/summary")
                        .param("from", "2026-08-12T00:00:00Z")
                        .param("to", "2026-08-11T00:00:00Z"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.type").value("urn:stream:alert:bad-request"));
    }

    @Test
    void timelineDefaultsItsLimitAndPassesTheSensorTypeThrough() throws Exception {
        when(queryService.timeline(anyString(), any(), anyInt())).thenReturn(List.of());

        mockMvc.perform(get("/api/v1/alerts/stations/{id}/timeline", "STN-1")
                        .param("sensorType", "RAINFALL"))
                .andExpect(status().isOk());

        verify(queryService).timeline("STN-1", SensorType.RAINFALL, 100);
    }

    @Test
    void aNonPositiveTimelineLimitIsRejected() throws Exception {
        mockMvc.perform(get("/api/v1/alerts/stations/{id}/timeline", "STN-1").param("limit", "0"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Validation failed"));
    }

    @Test
    void anUnexpectedFailureBecomesAnOpaqueFiveHundredWithACorrelationId() throws Exception {
        when(queryService.findById(anyString())).thenThrow(new IllegalStateException("boom"));

        mockMvc.perform(get("/api/v1/alerts/{id}", "abc"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.title").value("Internal error"))
                .andExpect(jsonPath("$.incidentId").isNotEmpty())
                // The cause must not cross the network.
                .andExpect(jsonPath("$.detail").value(
                        org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("boom"))));
    }

    private static PageResponse<AlertDto> emptyPage() {
        return new PageResponse<>(List.of(), 0, 50, 0L, 0);
    }

    private static AlertDto dto(String alertId) {
        return new AlertDto(alertId, "STN-1", "Station STN-1", "DIST-01",
                SensorType.RAINFALL, Severity.SEVERE, "RAINFALL_WINDOW_SUM",
                87.5d, 60.0d, MeasurementUnit.MM,
                Instant.parse("2026-08-11T06:00:00Z"), Instant.parse("2026-08-11T06:05:00Z"),
                "SEVERE at STN-1", 16.5062d, 80.6480d,
                Instant.parse("2026-08-11T06:05:02Z"), Instant.parse("2026-08-11T06:05:03Z"),
                false, null, null);
    }
}
