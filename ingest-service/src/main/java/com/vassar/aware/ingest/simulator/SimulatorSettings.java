package com.vassar.aware.ingest.simulator;

/**
 * The simulator's tuning, decoupled from Spring's configuration binding.
 *
 * <p>Keeping this a plain record means {@link ReadingSimulator} and {@link StormEpisodeTracker}
 * can be constructed and exercised in a unit test with no application context at all, which is
 * what makes the storm behaviour cheap enough to assert thoroughly.</p>
 */
public record SimulatorSettings(
        long seed,
        double stormFraction,
        int stormMinCycles,
        int stormMaxCycles,
        double jitterFraction,
        double stormRainfallMinMm,
        double stormRainfallMaxMm,
        double stormReservoirMinPercent,
        double stormReservoirMaxPercent,
        double stormRiverMinMetres,
        double stormRiverMaxMetres) {

    public SimulatorSettings {
        if (stormMinCycles < 1) {
            throw new IllegalArgumentException("stormMinCycles must be >= 1");
        }
        if (stormMaxCycles < stormMinCycles) {
            throw new IllegalArgumentException("stormMaxCycles must be >= stormMinCycles");
        }
        if (stormFraction < 0.0d || stormFraction > 1.0d) {
            throw new IllegalArgumentException("stormFraction must be within [0,1]");
        }
        if (jitterFraction < 0.0d) {
            throw new IllegalArgumentException("jitterFraction must be >= 0");
        }
    }

    /** Defaults matching {@code application.yml}, for tests that do not care about the tuning. */
    public static SimulatorSettings defaults() {
        return new SimulatorSettings(42L, 0.12d, 5, 10, 0.15d,
                4.0d, 12.0d, 86.0d, 101.5d, 8.2d, 12.6d);
    }

    public double meanStormCycles() {
        return (stormMinCycles + stormMaxCycles) / 2.0d;
    }
}
