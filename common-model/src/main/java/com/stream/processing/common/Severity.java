package com.stream.processing.common;

/**
 * Alert severity, ordered from least to most serious. {@link #rank()} is used to pick the
 * highest breached band when a value crosses several thresholds at once.
 */
public enum Severity {

    INFO(0),
    WARNING(1),
    SEVERE(2),
    EXTREME(3);

    private final int rank;

    Severity(int rank) {
        this.rank = rank;
    }

    public int rank() {
        return rank;
    }

    public boolean isAtLeast(Severity other) {
        return other != null && this.rank >= other.rank;
    }
}
