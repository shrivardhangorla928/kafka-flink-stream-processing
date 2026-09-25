package com.stream.processing.ingest.scrape;

import com.stream.processing.ingest.config.IngestProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.Collections;
import java.util.List;

@Component
public class DesweatherClient {

    private static final Logger LOG = LoggerFactory.getLogger(DesweatherClient.class);

    private final RestClient restClient;

    public DesweatherClient(IngestProperties properties) {
        this.restClient = RestClient.builder().baseUrl(properties.getDesweatherUrl()).build();
    }

    public List<DesweatherReading> fetch() {
        try {
            List<DesweatherReading> readings = restClient.get()
                    .retrieve()
                    .body(new ParameterizedTypeReference<List<DesweatherReading>>() {
                    });
            return readings == null ? Collections.emptyList() : readings;
        } catch (Exception e) {
            LOG.warn("Failed to fetch desweather feed: {}", e.getMessage());
            return Collections.emptyList();
        }
    }
}
