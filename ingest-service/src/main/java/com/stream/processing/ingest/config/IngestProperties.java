package com.stream.processing.ingest.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "stream.ingest")
public class IngestProperties {

    @NotBlank
    private String desweatherUrl = "http://desweather.ap.gov.in/webservice/rest/json";

    @Positive
    private long desweatherPollIntervalMs = 300_000L;

    // off in tests, so a Spring context boot never fires a real HTTP call against the live API
    private boolean desweatherScrapeEnabled = true;

    @Min(1)
    private int maxBatchSize = 1_000;

    public String getDesweatherUrl() {
        return desweatherUrl;
    }

    public void setDesweatherUrl(String desweatherUrl) {
        this.desweatherUrl = desweatherUrl;
    }

    public long getDesweatherPollIntervalMs() {
        return desweatherPollIntervalMs;
    }

    public void setDesweatherPollIntervalMs(long desweatherPollIntervalMs) {
        this.desweatherPollIntervalMs = desweatherPollIntervalMs;
    }

    public boolean isDesweatherScrapeEnabled() {
        return desweatherScrapeEnabled;
    }

    public void setDesweatherScrapeEnabled(boolean desweatherScrapeEnabled) {
        this.desweatherScrapeEnabled = desweatherScrapeEnabled;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }
}
