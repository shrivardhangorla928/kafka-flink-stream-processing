package com.stream.processing.ingest.config;

import com.stream.processing.ingest.simulator.SimulatorSettings;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The configuration is the pipeline's contract with its operator, so the defaults and the guards
 * against nonsensical combinations are asserted rather than assumed. A storm band that cannot
 * breach the downstream threshold would produce a demonstration with no alerts in it and no error
 * anywhere - exactly the kind of silent failure the validation exists to prevent.
 */
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

        assertThat(properties.getScrapeIntervalMs()).isEqualTo(60_000L);
        assertThat(properties.getSource()).isEqualTo("STREAM_SIMULATOR");
        assertThat(properties.getStationsResource()).isEqualTo("classpath:stations.json");
        assertThat(properties.getMaxBatchSize()).isEqualTo(1_000);
        assertThat(properties.getMaxBurstStormStations()).isEqualTo(50);

        IngestProperties.SimulatorProperties simulator = properties.getSimulator();
        assertThat(simulator.isEnabled()).isTrue();
        assertThat(simulator.getSeed()).isEqualTo(42L);
        assertThat(simulator.getStormFraction()).isEqualTo(0.12d);
        assertThat(VALIDATOR.validate(properties)).isEmpty();
    }

    @Test
    @DisplayName("the default storm band lands a 5-reading window inside the alerting thresholds")
    void defaultStormBandCanBreachTheDownstreamThresholds() {
        IngestProperties.SimulatorProperties simulator = new IngestProperties().getSimulator();

        // 5 readings per 5-minute window at the default 60 s scrape interval
        assertThat(simulator.getStormRainfallMinMm() * 5).isGreaterThanOrEqualTo(15.0d);
        assertThat(simulator.getStormReservoirMinPercent()).isGreaterThanOrEqualTo(85.0d);
        assertThat(simulator.getStormRiverMinMetres()).isGreaterThanOrEqualTo(8.0d);
    }

    @Test
    @DisplayName("the bound properties project cleanly onto the simulator's settings")
    void propertiesProjectOntoSimulatorSettings() {
        IngestProperties.SimulatorProperties simulator = new IngestProperties().getSimulator();
        simulator.setSeed(7L);
        simulator.setStormFraction(0.25d);
        simulator.setStormMinCycles(2);
        simulator.setStormMaxCycles(4);
        simulator.setJitterFraction(0.2d);
        simulator.setStormRainfallMinMm(5.0d);
        simulator.setStormRainfallMaxMm(9.0d);
        simulator.setStormReservoirMinPercent(90.0d);
        simulator.setStormReservoirMaxPercent(99.0d);
        simulator.setStormRiverMinMetres(9.0d);
        simulator.setStormRiverMaxMetres(11.0d);

        SimulatorSettings settings = simulator.toSettings();

        assertThat(settings).isEqualTo(new SimulatorSettings(7L, 0.25d, 2, 4, 0.2d,
                5.0d, 9.0d, 90.0d, 99.0d, 9.0d, 11.0d));
    }

    @Test
    @DisplayName("an inverted cycle range is rejected")
    void invertedCycleRangeIsRejected() {
        IngestProperties properties = new IngestProperties();
        properties.getSimulator().setStormMinCycles(9);
        properties.getSimulator().setStormMaxCycles(3);

        assertThat(properties.getSimulator().isCycleRangeOrdered()).isFalse();
        assertThat(VALIDATOR.validate(properties)).hasSize(1);
    }

    @Test
    @DisplayName("an inverted storm band is rejected")
    void invertedStormBandIsRejected() {
        IngestProperties properties = new IngestProperties();
        properties.getSimulator().setStormRainfallMinMm(12.0d);
        properties.getSimulator().setStormRainfallMaxMm(4.0d);

        assertThat(properties.getSimulator().isStormBandOrdered()).isFalse();
        assertThat(VALIDATOR.validate(properties)).hasSize(1);
    }

    @Test
    @DisplayName("impossible scalar values are rejected")
    void impossibleScalarsAreRejected() {
        IngestProperties properties = new IngestProperties();
        properties.setScrapeIntervalMs(0L);
        properties.setSource("  ");
        properties.setMaxBatchSize(0);
        properties.setMaxBurstStormStations(0);
        properties.getSimulator().setStormFraction(1.5d);

        assertThat(VALIDATOR.validate(properties)).hasSize(5);
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
