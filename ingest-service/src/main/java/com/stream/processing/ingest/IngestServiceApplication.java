package com.stream.processing.ingest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point of the ingest service, the "scraper" stage of the pipeline.
 *
 * <p>Scheduling is enabled here rather than in a nested configuration because the periodic
 * scrape is the service's primary reason to exist; the simulator behind it can still be
 * switched off with {@code stream.ingest.simulator.enabled=false} when the service is driven
 * purely over REST.</p>
 */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class IngestServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(IngestServiceApplication.class, args);
    }
}
