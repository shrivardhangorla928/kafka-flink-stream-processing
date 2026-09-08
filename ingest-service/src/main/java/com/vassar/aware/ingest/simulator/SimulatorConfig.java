package com.vassar.aware.ingest.simulator;

import com.vassar.aware.ingest.config.IngestProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Bridges the Spring-bound properties to the framework-free {@link ReadingSimulator}.
 *
 * <p>The simulator is built here instead of being annotated {@code @Component} so that it stays
 * free of any framework import and remains directly constructible in a unit test.</p>
 */
@Configuration(proxyBeanMethods = false)
public class SimulatorConfig {

    @Bean
    public ReadingSimulator readingSimulator(IngestProperties properties) {
        return new ReadingSimulator(properties.getSimulator().toSettings(), properties.getSource());
    }
}
