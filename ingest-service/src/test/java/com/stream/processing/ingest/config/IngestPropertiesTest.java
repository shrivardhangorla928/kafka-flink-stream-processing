package com.stream.processing.ingest.config;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IngestPropertiesTest {

    private static final Validator VALIDATOR;

    static {
        try (ValidatorFactory factory = Validation.buildDefaultValidatorFactory()) {
            VALIDATOR = factory.getValidator();
        }
    }

    @Test
    @DisplayName("the defaults match the values documented in application.yml")
    void defaultsMatchTheDocumentedConfiguration() {
        IngestProperties properties = new IngestProperties();

        assertThat(properties.getDesweatherUrl()).isEqualTo("http://desweather.ap.gov.in/webservice/rest/json");
        assertThat(properties.getDesweatherPollIntervalMs()).isEqualTo(300_000L);
        assertThat(properties.getMaxBatchSize()).isEqualTo(1_000);
        assertThat(VALIDATOR.validate(properties)).isEmpty();
    }

    @Test
    @DisplayName("impossible scalar values are rejected")
    void impossibleScalarsAreRejected() {
        IngestProperties properties = new IngestProperties();
        properties.setDesweatherUrl("  ");
        properties.setDesweatherPollIntervalMs(0L);
        properties.setMaxBatchSize(0);

        assertThat(VALIDATOR.validate(properties)).hasSize(3);
    }

    @Test
    @DisplayName("topic geometry defaults to three partitions on a single broker")
    void topicPropertiesDefaults() {
        KafkaTopicProperties topics = new KafkaTopicProperties();

        assertThat(topics.getPartitions()).isEqualTo(3);
        assertThat(topics.getReplicationFactor()).isEqualTo((short) 1);
        assertThat(VALIDATOR.validate(topics)).isEmpty();

        topics.setPartitions(0);
        topics.setReplicationFactor((short) 0);
        assertThat(VALIDATOR.validate(topics)).hasSize(2);
    }
}
