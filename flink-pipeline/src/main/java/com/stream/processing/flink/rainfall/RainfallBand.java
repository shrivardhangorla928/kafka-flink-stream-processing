package com.stream.processing.flink.rainfall;

import java.io.Serializable;

/**
 * One rainfall-intensity band for a specific accumulation window: a half-open range
 * {@code [minInclusive, maxExclusive)} of accumulated millimetres, tagged with a
 * machine-readable label such as {@code "heavy_rainfall"}.
 *
 * <p>Bands are defined per accumulation window (1, 2, 3, 6, 12 or 24 hours) because the same
 * millimetre reading means something different depending on how long it took to fall; see
 * {@link RainfallAlertPolicy} for the full per-window tables.</p>
 */
public record RainfallBand(String label, double minInclusive, double maxExclusive) implements Serializable {

    public boolean matches(double value) {
        return value >= minInclusive && value < maxExclusive;
    }
}
