package com.stream.processing.alert.web;

import com.stream.processing.alert.AbstractIntegrationTest;
import com.stream.processing.alert.AlertFixtures;
import com.stream.processing.alert.domain.AlertEntity;
import com.stream.processing.alert.mapper.AlertMapper;
import com.stream.processing.alert.mapper.StationWindowAggregateMapper;
import com.stream.processing.common.Alert;
import com.stream.processing.common.SensorType;
import com.stream.processing.common.Severity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The query API exercised end to end - HTTP in, real SQL against the Flyway schema, JSON out.
 *
 * <p>The semantics that matter here cannot be proved with a mocked service: minimum-severity
 * filtering only works if the denormalised rank comparison reaches the database correctly, the
 * page cap only holds if the clamp is applied before the query, and the default ordering only
 * holds if the sort survives into the SQL.</p>
 */
class AlertApiIntegrationTest extends AbstractIntegrationTest {

    private static final Instant T0 = Instant.parse("2026-08-11T06:00:00Z");
    private static final String BASE = "/api/v1/alerts";

    @Autowired
    private AlertMapper alertMapper;

    @Autowired
    private StationWindowAggregateMapper aggregateMapper;

    @BeforeEach
    void seed() {
        alertRepository.saveAllAndFlush(List.of(
                row("STN-1", SensorType.RAINFALL, Severity.INFO, "DIST-01", T0, false),
                row("STN-1", SensorType.RAINFALL, Severity.WARNING, "DIST-01", T0.plusSeconds(60), false),
                row("STN-2", SensorType.RIVER_LEVEL, Severity.SEVERE, "DIST-01", T0.plusSeconds(120), true),
                row("STN-3", SensorType.RESERVOIR_LEVEL, Severity.EXTREME, "DIST-02", T0.plusSeconds(180), false),
                row("STN-4", SensorType.RAINFALL, Severity.WARNING, "DIST-02", T0.plusSeconds(240), false)));

        aggregateRepository.saveAllAndFlush(List.of(
                aggregateMapper.toEntity(AlertFixtures.aggregate(
                        "STN-1", SensorType.RAINFALL, T0, 12d), T0),
                aggregateMapper.toEntity(AlertFixtures.aggregate(
                        "STN-1", SensorType.RAINFALL, T0.plusSeconds(300), 24d), T0),
                aggregateMapper.toEntity(AlertFixtures.aggregate(
                        "STN-1", SensorType.RIVER_LEVEL, T0.plusSeconds(300), 4d), T0)));
    }

    @Test
    void listsEveryAlertNewestFirstByDefault() throws Exception {
        mockMvc.perform(get(BASE))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElements").value(5))
                .andExpect(jsonPath("$.content.length()").value(5))
                .andExpect(jsonPath("$.content[0].generatedAt").value("2026-08-11T06:04:00Z"))
                .andExpect(jsonPath("$.content[4].generatedAt").value("2026-08-11T06:00:00Z"));
    }

    @Test
    void severityIsAMinimumSoWarningReturnsWarningSevereAndExtreme() throws Exception {
        mockMvc.perform(get(BASE).param("severity", "WARNING"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElements").value(4))
                .andExpect(jsonPath("$.content[*].severity",
                        org.hamcrest.Matchers.everyItem(
                                org.hamcrest.Matchers.not(org.hamcrest.Matchers.is("INFO")))));
    }

    @Test
    void severeReturnsOnlySevereAndExtreme() throws Exception {
        mockMvc.perform(get(BASE).param("severity", "SEVERE"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElements").value(2))
                .andExpect(jsonPath("$.content[*].severity",
                        org.hamcrest.Matchers.containsInAnyOrder("EXTREME", "SEVERE")));
    }

    @Test
    void theLowestBandReturnsEverything() throws Exception {
        mockMvc.perform(get(BASE).param("severity", "INFO"))
                .andExpect(jsonPath("$.totalElements").value(5));
    }

    @Test
    void filtersByDistrict() throws Exception {
        mockMvc.perform(get(BASE).param("districtId", "DIST-02"))
                .andExpect(jsonPath("$.totalElements").value(2))
                .andExpect(jsonPath("$.content[*].districtId",
                        org.hamcrest.Matchers.everyItem(org.hamcrest.Matchers.is("DIST-02"))));
    }

    @Test
    void filtersByStationAndSensorType() throws Exception {
        mockMvc.perform(get(BASE)
                        .param("stationId", "STN-1")
                        .param("sensorType", "RAINFALL"))
                .andExpect(jsonPath("$.totalElements").value(2));
    }

    @Test
    void filtersByAcknowledgementState() throws Exception {
        mockMvc.perform(get(BASE).param("acknowledged", "true"))
                .andExpect(jsonPath("$.totalElements").value(1))
                .andExpect(jsonPath("$.content[0].stationId").value("STN-2"));
        mockMvc.perform(get(BASE).param("acknowledged", "false"))
                .andExpect(jsonPath("$.totalElements").value(4));
    }

    @Test
    void filtersByAHalfOpenTimeRange() throws Exception {
        mockMvc.perform(get(BASE)
                        .param("from", "2026-08-11T06:01:00Z")
                        .param("to", "2026-08-11T06:03:00Z"))
                .andExpect(jsonPath("$.totalElements").value(2));
    }

    @Test
    void everyFilterComposesIntoOneQuery() throws Exception {
        mockMvc.perform(get(BASE)
                        .param("districtId", "DIST-02")
                        .param("sensorType", "RAINFALL")
                        .param("severity", "WARNING")
                        .param("from", "2026-08-11T06:00:00Z")
                        .param("to", "2026-08-11T07:00:00Z")
                        .param("acknowledged", "false"))
                .andExpect(jsonPath("$.totalElements").value(1))
                .andExpect(jsonPath("$.content[0].stationId").value("STN-4"));
    }

    @Test
    void aFilterCombinationThatMatchesNothingReturnsAnEmptyPageNotAnError() throws Exception {
        mockMvc.perform(get(BASE)
                        .param("districtId", "DIST-99")
                        .param("severity", "EXTREME"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElements").value(0))
                .andExpect(jsonPath("$.content.length()").value(0));
    }

    @Test
    void theRequestedPageSizeIsCappedServerSide() throws Exception {
        mockMvc.perform(get(BASE).param("size", "100000"))
                .andExpect(status().isOk())
                // stream.alert.api.max-page-size, not the 100000 the caller asked for.
                .andExpect(jsonPath("$.size").value(200));
    }

    @Test
    void aPageSizeBelowTheCapIsHonoured() throws Exception {
        mockMvc.perform(get(BASE).param("size", "2").param("page", "1"))
                .andExpect(jsonPath("$.size").value(2))
                .andExpect(jsonPath("$.page").value(1))
                .andExpect(jsonPath("$.totalPages").value(3))
                .andExpect(jsonPath("$.content.length()").value(2));
    }

    @Test
    void anExplicitSortOverridesTheDefault() throws Exception {
        mockMvc.perform(get(BASE).param("sort", "generatedAt,asc"))
                .andExpect(jsonPath("$.content[0].generatedAt").value("2026-08-11T06:00:00Z"));
    }

    @Test
    void singleAlertIsFetchedById() throws Exception {
        String alertId = alertRepository.findAll().getFirst().getAlertId();

        mockMvc.perform(get(BASE + "/{id}", alertId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.alertId").value(alertId))
                .andExpect(jsonPath("$.receivedAt").isNotEmpty());
    }

    @Test
    void anUnknownAlertIdIsANotFound() throws Exception {
        mockMvc.perform(get(BASE + "/{id}", "00000000-0000-0000-0000-000000000000"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.title").value("Alert not found"));
    }

    @Test
    void acknowledgingIsIdempotentAndKeepsTheFirstResponder() throws Exception {
        String alertId = alertRepository.findAll().stream()
                .filter(row -> !row.isAcknowledged())
                .findFirst().orElseThrow().getAlertId();

        String firstResponse = mockMvc.perform(post(BASE + "/{id}/acknowledge", alertId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"acknowledgedBy\":\"duty-officer\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.acknowledged").value(true))
                .andExpect(jsonPath("$.acknowledgedBy").value("duty-officer"))
                .andReturn().getResponse().getContentAsString();

        String acknowledgedAt = com.jayway.jsonpath.JsonPath.read(firstResponse, "$.acknowledgedAt");

        mockMvc.perform(post(BASE + "/{id}/acknowledge", alertId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"acknowledgedBy\":\"someone-else\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.acknowledged").value(true))
                // The audit trail belongs to whoever got there first.
                .andExpect(jsonPath("$.acknowledgedBy").value("duty-officer"))
                .andExpect(jsonPath("$.acknowledgedAt").value(acknowledgedAt));

        assertThat(alertRepository.findById(alertId).orElseThrow().getAcknowledgedBy())
                .isEqualTo("duty-officer");
    }

    @Test
    void summaryCountsBySeverityAndDistrictOverTheRange() throws Exception {
        mockMvc.perform(get(BASE + "/summary")
                        .param("from", "2026-08-11T06:00:00Z")
                        .param("to", "2026-08-11T07:00:00Z"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(5))
                .andExpect(jsonPath("$.bySeverity.INFO").value(1))
                .andExpect(jsonPath("$.bySeverity.WARNING").value(2))
                .andExpect(jsonPath("$.bySeverity.SEVERE").value(1))
                .andExpect(jsonPath("$.bySeverity.EXTREME").value(1))
                .andExpect(jsonPath("$.byDistrict.length()").value(2))
                // Highest first, so a dashboard can take the top N without sorting.
                .andExpect(jsonPath("$.byDistrict[0].districtId").value("DIST-01"))
                .andExpect(jsonPath("$.byDistrict[0].count").value(3));
    }

    @Test
    void summaryKeepsEmptySeverityBandsSoTheChartShapeIsStable() throws Exception {
        mockMvc.perform(get(BASE + "/summary")
                        .param("from", "2020-01-01T00:00:00Z")
                        .param("to", "2020-01-02T00:00:00Z"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(0))
                .andExpect(jsonPath("$.bySeverity.INFO").value(0))
                .andExpect(jsonPath("$.bySeverity.EXTREME").value(0))
                .andExpect(jsonPath("$.byDistrict.length()").value(0));
    }

    @Test
    void stationTimelineReturnsRecentWindowsNewestFirst() throws Exception {
        mockMvc.perform(get(BASE + "/stations/{id}/timeline", "STN-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(3))
                .andExpect(jsonPath("$[0].windowStart").value("2026-08-11T06:05:00Z"));
    }

    @Test
    void stationTimelineCanBeNarrowedToOneSensorType() throws Exception {
        mockMvc.perform(get(BASE + "/stations/{id}/timeline", "STN-1")
                        .param("sensorType", "RIVER_LEVEL"))
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].aggregatedValue").value(4.0d));
    }

    @Test
    void stationTimelineHonoursItsLimit() throws Exception {
        mockMvc.perform(get(BASE + "/stations/{id}/timeline", "STN-1").param("limit", "1"))
                .andExpect(jsonPath("$.length()").value(1));
    }

    @Test
    void anUnknownStationTimelineIsAnEmptyListNotAnError() throws Exception {
        mockMvc.perform(get(BASE + "/stations/{id}/timeline", "NOPE"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test
    void prometheusExposesThePipelineLatencyAndAlertCounters() throws Exception {
        // The evaluation chapter scrapes these names; a rename would silently empty the panels.
        String scrape = mockMvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        assertThat(scrape).contains("stream_alerts_pipeline_latency_seconds");
        assertThat(scrape).contains("stream_alerts_duplicate_total");
        assertThat(scrape).contains("stream_alerts_persist_failures_total");
        assertThat(scrape).contains("application=\"alert-service\"");
    }

    @Test
    void kubernetesProbesAreExposed() throws Exception {
        mockMvc.perform(get("/actuator/health/liveness")).andExpect(status().isOk());
        mockMvc.perform(get("/actuator/health/readiness")).andExpect(status().isOk());
    }

    private AlertEntity row(String stationId,
                            SensorType sensorType,
                            Severity severity,
                            String districtId,
                            Instant generatedAt,
                            boolean acknowledged) {
        Alert alert = AlertFixtures.alert(stationId, sensorType, severity, generatedAt, districtId);
        AlertEntity entity = alertMapper.toEntity(alert, generatedAt.plusSeconds(1));
        entity.setGeneratedAt(generatedAt);
        if (acknowledged) {
            entity.acknowledge("ops", generatedAt.plusSeconds(30));
        }
        return entity;
    }
}
