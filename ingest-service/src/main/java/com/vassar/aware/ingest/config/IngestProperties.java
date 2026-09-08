package com.vassar.aware.ingest.config;

import com.vassar.aware.ingest.simulator.SimulatorSettings;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Every operator-tunable number the ingest service uses, bound from {@code aware.ingest.*}.
 *
 * <p>The values are gathered in one validated type instead of being scattered across
 * {@code @Value} annotations so that a bad combination (for instance a storm band that cannot
 * breach the downstream thresholds, or a max-cycle count below the min) fails fast at start-up
 * rather than silently producing a demo with no alerts in it.</p>
 */
@Validated
@ConfigurationProperties(prefix = "aware.ingest")
public class IngestProperties {

    /** How often the scheduled scraper produces a full round of readings. */
    @Positive
    private long scrapeIntervalMs = 60_000L;

    /** Value written into {@code SensorReading.source} so downstream stages can tell scrapers apart. */
    @NotBlank
    private String source = "AWARE_SIMULATOR";

    /** Spring resource location of the station catalogue. */
    @NotBlank
    private String stationsResource = "classpath:stations.json";

    /** Upper bound on the size of a single {@code POST /readings/batch} payload. */
    @Min(1)
    private int maxBatchSize = 1_000;

    /** Upper bound on the number of stations a burst request may force into a storm. */
    @Min(1)
    private int maxBurstStormStations = 50;

    @Valid
    private final SimulatorProperties simulator = new SimulatorProperties();

    public long getScrapeIntervalMs() {
        return scrapeIntervalMs;
    }

    public void setScrapeIntervalMs(long scrapeIntervalMs) {
        this.scrapeIntervalMs = scrapeIntervalMs;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getStationsResource() {
        return stationsResource;
    }

    public void setStationsResource(String stationsResource) {
        this.stationsResource = stationsResource;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public int getMaxBurstStormStations() {
        return maxBurstStormStations;
    }

    public void setMaxBurstStormStations(int maxBurstStormStations) {
        this.maxBurstStormStations = maxBurstStormStations;
    }

    public SimulatorProperties getSimulator() {
        return simulator;
    }

    /**
     * Tuning of the synthetic station behaviour.
     *
     * <p>The storm bands are deliberately expressed in the same units the downstream Flink
     * thresholds use, so the relationship between the two is auditable: with a 60 s scrape
     * interval a 5-minute tumbling window holds five readings, and 4-12 mm each sums to
     * 20-60 mm, spanning the WARNING (15 mm), SEVERE (30 mm) and EXTREME (50 mm) bands.</p>
     */
    public static class SimulatorProperties {

        /** Whether the scheduled scrape round runs at all. */
        private boolean enabled = true;

        /** Seed of the single {@link java.util.Random} the simulator draws from, for reproducible runs. */
        private long seed = 42L;

        /** Steady-state fraction of stations expected to be inside a storm episode. */
        @DecimalMin("0.0")
        @DecimalMax("1.0")
        private double stormFraction = 0.12d;

        /** Shortest storm episode, in scrape cycles. */
        @Min(1)
        private int stormMinCycles = 5;

        /** Longest storm episode, in scrape cycles. */
        @Min(1)
        private int stormMaxCycles = 10;

        /** Relative spread applied to a station's baseline while it is behaving normally. */
        @DecimalMin("0.0")
        @DecimalMax("1.0")
        private double jitterFraction = 0.15d;

        @DecimalMin("0.0")
        private double stormRainfallMinMm = 4.0d;

        @DecimalMin("0.0")
        private double stormRainfallMaxMm = 12.0d;

        @DecimalMin("0.0")
        private double stormReservoirMinPercent = 86.0d;

        @DecimalMin("0.0")
        private double stormReservoirMaxPercent = 101.5d;

        @DecimalMin("0.0")
        private double stormRiverMinMetres = 8.2d;

        @DecimalMin("0.0")
        private double stormRiverMaxMetres = 12.6d;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public long getSeed() {
            return seed;
        }

        public void setSeed(long seed) {
            this.seed = seed;
        }

        public double getStormFraction() {
            return stormFraction;
        }

        public void setStormFraction(double stormFraction) {
            this.stormFraction = stormFraction;
        }

        public int getStormMinCycles() {
            return stormMinCycles;
        }

        public void setStormMinCycles(int stormMinCycles) {
            this.stormMinCycles = stormMinCycles;
        }

        public int getStormMaxCycles() {
            return stormMaxCycles;
        }

        public void setStormMaxCycles(int stormMaxCycles) {
            this.stormMaxCycles = stormMaxCycles;
        }

        public double getJitterFraction() {
            return jitterFraction;
        }

        public void setJitterFraction(double jitterFraction) {
            this.jitterFraction = jitterFraction;
        }

        public double getStormRainfallMinMm() {
            return stormRainfallMinMm;
        }

        public void setStormRainfallMinMm(double stormRainfallMinMm) {
            this.stormRainfallMinMm = stormRainfallMinMm;
        }

        public double getStormRainfallMaxMm() {
            return stormRainfallMaxMm;
        }

        public void setStormRainfallMaxMm(double stormRainfallMaxMm) {
            this.stormRainfallMaxMm = stormRainfallMaxMm;
        }

        public double getStormReservoirMinPercent() {
            return stormReservoirMinPercent;
        }

        public void setStormReservoirMinPercent(double stormReservoirMinPercent) {
            this.stormReservoirMinPercent = stormReservoirMinPercent;
        }

        public double getStormReservoirMaxPercent() {
            return stormReservoirMaxPercent;
        }

        public void setStormReservoirMaxPercent(double stormReservoirMaxPercent) {
            this.stormReservoirMaxPercent = stormReservoirMaxPercent;
        }

        public double getStormRiverMinMetres() {
            return stormRiverMinMetres;
        }

        public void setStormRiverMinMetres(double stormRiverMinMetres) {
            this.stormRiverMinMetres = stormRiverMinMetres;
        }

        public double getStormRiverMaxMetres() {
            return stormRiverMaxMetres;
        }

        public void setStormRiverMaxMetres(double stormRiverMaxMetres) {
            this.stormRiverMaxMetres = stormRiverMaxMetres;
        }

        @AssertTrue(message = "aware.ingest.simulator.storm-max-cycles must be >= storm-min-cycles")
        public boolean isCycleRangeOrdered() {
            return stormMaxCycles >= stormMinCycles;
        }

        @AssertTrue(message = "every aware.ingest.simulator storm band must have max >= min")
        public boolean isStormBandOrdered() {
            return stormRainfallMaxMm >= stormRainfallMinMm
                    && stormReservoirMaxPercent >= stormReservoirMinPercent
                    && stormRiverMaxMetres >= stormRiverMinMetres;
        }

        /** Projects the bound properties onto the framework-free record the simulator consumes. */
        public SimulatorSettings toSettings() {
            return new SimulatorSettings(seed, stormFraction, stormMinCycles, stormMaxCycles, jitterFraction,
                    stormRainfallMinMm, stormRainfallMaxMm, stormReservoirMinPercent, stormReservoirMaxPercent,
                    stormRiverMinMetres, stormRiverMaxMetres);
        }
    }
}
