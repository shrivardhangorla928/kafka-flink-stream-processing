package com.vassar.aware.alert;

import com.vassar.aware.alert.config.AlertServiceProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Sink end of the AWARE pipeline: consumes the alerts the Flink threshold stage publishes,
 * persists them idempotently and serves the read API the dashboard and Grafana query.
 *
 * <p>Kept as a plain Spring Boot service rather than folding the store into the Flink job so
 * that the query path can be scaled, deployed and rolled back independently of the streaming
 * topology - the whole point of the deployment-automation chapter.</p>
 */
@SpringBootApplication
@EnableConfigurationProperties(AlertServiceProperties.class)
public class AlertServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AlertServiceApplication.class, args);
    }
}
