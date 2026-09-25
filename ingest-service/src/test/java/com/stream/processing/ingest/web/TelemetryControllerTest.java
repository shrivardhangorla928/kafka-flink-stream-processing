package com.stream.processing.ingest.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stream.processing.common.JsonCodec;
import com.stream.processing.common.SensorReading;
import com.stream.processing.common.SensorType;
import com.stream.processing.common.Topics;
import com.stream.processing.ingest.config.IngestProperties;
import com.stream.processing.ingest.publish.TelemetryPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(TelemetryController.class)
@TestPropertySource(properties = "stream.ingest.max-batch-size=3")
class TelemetryControllerTest {

    private static final String BASE = "/api/v1/telemetry";
    private static final MediaType PROBLEM_JSON = MediaType.APPLICATION_PROBLEM_JSON;
    private static final int MAX_BATCH = 3;

    private final ObjectMapper json = JsonCodec.create();

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private TelemetryPublisher publisher;

    @TestConfiguration
    static class IngestPropertiesConfiguration {

        @Bean
        IngestProperties ingestProperties() {
            return new IngestProperties();
        }
    }

    @BeforeEach
    void setUp() {
        when(publisher.publish(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(publisher.publishAll(anyCollection()))
                .thenAnswer(invocation -> ((List<?>) invocation.getArgument(0)).size());
    }

    private static Map<String, Object> readingPayload() {
        return Map.of(
                "stationId", "STN-0001",
                "sensorType", "RAINFALL",
                "value", 6.5,
                "eventTime", "2026-08-11T06:00:00Z");
    }

    private String asJson(Object body) throws Exception {
        return json.writeValueAsString(body);
    }

    @Test
    @DisplayName("POST /readings accepts a reading and answers 202")
    void postReadingIsAccepted() throws Exception {
        mockMvc.perform(post(BASE + "/readings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(asJson(readingPayload())))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.accepted").value(1))
                .andExpect(jsonPath("$.topic").value(Topics.RAW_TELEMETRY))
                .andExpect(jsonPath("$.acceptedAt").exists());

        ArgumentCaptor<SensorReading> captor = ArgumentCaptor.forClass(SensorReading.class);
        verify(publisher).publish(captor.capture());
        SensorReading published = captor.getValue();
        assertThat(published.getStationId()).isEqualTo("STN-0001");
        assertThat(published.getSensorType()).isEqualTo(SensorType.RAINFALL);
        assertThat(published.getValue()).isEqualTo(6.5d);
        assertThat(published.getEventTime()).isEqualTo(Instant.parse("2026-08-11T06:00:00Z"));
        assertThat(published.getReadingId()).as("generated when the caller omits one").isNotBlank();
        assertThat(published.getSource()).isEqualTo(SensorReadingRequest.DEFAULT_SOURCE);
        assertThat(published.getUnit()).isEqualTo(SensorType.RAINFALL.getUnit());
    }

    @Test
    @DisplayName("POST /readings defaults the event time when the caller reports a current value")
    void postReadingDefaultsEventTime() throws Exception {
        mockMvc.perform(post(BASE + "/readings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(asJson(Map.of("stationId", "STN-0002", "sensorType", "RAINFALL",
                                "value", 9.2, "readingId", "client-supplied-1"))))
                .andExpect(status().isAccepted());

        ArgumentCaptor<SensorReading> captor = ArgumentCaptor.forClass(SensorReading.class);
        verify(publisher).publish(captor.capture());
        assertThat(captor.getValue().getEventTime()).isNotNull();
        assertThat(captor.getValue().getReadingId()).isEqualTo("client-supplied-1");
    }

    @Test
    @DisplayName("POST /readings rejects a missing station id with a problem document")
    void postReadingRejectsMissingStationId() throws Exception {
        mockMvc.perform(post(BASE + "/readings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(asJson(Map.of("sensorType", "RAINFALL", "value", 1.0))))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(PROBLEM_JSON))
                .andExpect(jsonPath("$.title").value("Validation failed"))
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.errors.stationId").value("stationId is required"))
                .andExpect(jsonPath("$.timestamp").exists());

        verify(publisher, never()).publish(any());
    }

    @Test
    @DisplayName("POST /readings rejects a negative value")
    void postReadingRejectsNegativeValue() throws Exception {
        mockMvc.perform(post(BASE + "/readings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(asJson(Map.of("stationId", "STN-0001", "sensorType", "RAINFALL",
                                "value", -4.0))))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.value").value("value must not be negative"));

        verify(publisher, never()).publish(any());
    }

    @Test
    @DisplayName("POST /readings rejects an out-of-range coordinate")
    void postReadingRejectsImpossibleCoordinates() throws Exception {
        mockMvc.perform(post(BASE + "/readings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(asJson(Map.of("stationId", "STN-0001", "sensorType", "RAINFALL",
                                "value", 1.0, "latitude", 120.0))))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.latitude").value("latitude must be within [-90,90]"));
    }

    @Test
    @DisplayName("POST /readings rejects a sensor type the pipeline does not know")
    void postReadingRejectsUnknownSensorType() throws Exception {
        mockMvc.perform(post(BASE + "/readings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"stationId\":\"STN-0001\",\"sensorType\":\"SNOWFALL\",\"value\":1.0}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Malformed request"));

        verify(publisher, never()).publish(any());
    }

    @Test
    @DisplayName("POST /readings rejects unparseable JSON")
    void postReadingRejectsMalformedJson() throws Exception {
        mockMvc.perform(post(BASE + "/readings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"stationId\": "))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Malformed request"));
    }

    @Test
    @DisplayName("POST /readings/batch accepts a list and reports how many were taken")
    void postBatchIsAccepted() throws Exception {
        mockMvc.perform(post(BASE + "/readings/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(asJson(List.of(readingPayload(), readingPayload()))))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.accepted").value(2));
    }

    @Test
    @DisplayName("POST /readings/batch rejects an empty list")
    void postBatchRejectsEmptyList() throws Exception {
        mockMvc.perform(post(BASE + "/readings/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("[]"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value("Batch must contain at least one reading"));

        verify(publisher, never()).publishAll(anyCollection());
    }

    @Test
    @DisplayName("POST /readings/batch rejects a batch larger than the configured cap")
    void postBatchRejectsOversizedBatch() throws Exception {
        List<Map<String, Object>> tooMany = List.of(readingPayload(), readingPayload(),
                readingPayload(), readingPayload());

        mockMvc.perform(post(BASE + "/readings/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(asJson(tooMany)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail")
                        .value("Batch of 4 exceeds stream.ingest.max-batch-size=" + MAX_BATCH));

        verify(publisher, never()).publishAll(anyCollection());
    }

    @Test
    @DisplayName("POST /readings/batch rejects the whole batch when one entry is invalid")
    void postBatchRejectsInvalidElement() throws Exception {
        List<Map<String, Object>> mixed = List.of(readingPayload(),
                Map.of("sensorType", "RAINFALL", "value", 2.0));

        mockMvc.perform(post(BASE + "/readings/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(asJson(mixed)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Validation failed"));

        verify(publisher, never()).publishAll(anyCollection());
    }
}
