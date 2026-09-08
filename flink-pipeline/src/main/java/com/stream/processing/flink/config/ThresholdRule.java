package com.stream.processing.flink.config;

import com.stream.processing.common.Severity;

import java.io.Serializable;
import java.util.Objects;

/**
 * The three severity bands one sensor type is evaluated against, plus the name of the metric
 * that is being compared.
 *
 * <p>Kept free of any Flink type on purpose: threshold policy is the part of this job a domain
 * expert will want to argue about, so it has to be readable and unit-testable without starting
 * a cluster. The operator that uses it is a thin wrapper.</p>
 *
 * <p>Bands are inclusive lower bounds — a value exactly equal to the warning threshold is a
 * breach. Hydrological warning levels are published as "at or above", and treating the boundary
 * as safe would silently swallow the very reading an operator calibrated the gauge to catch.</p>
 */
public final class ThresholdRule implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String metric;
    private final double warning;
    private final double severe;
    private final double extreme;

    public ThresholdRule(String metric, double warning, double severe, double extreme) {
        this.metric = Objects.requireNonNull(metric, "metric");
        if (Double.isNaN(warning) || Double.isNaN(severe) || Double.isNaN(extreme)) {
            throw new IllegalArgumentException("Thresholds for " + metric + " must be numbers");
        }
        if (!(warning <= severe && severe <= extreme)) {
            throw new IllegalArgumentException("Thresholds for " + metric
                    + " must be non-decreasing but were warning=" + warning
                    + ", severe=" + severe + ", extreme=" + extreme);
        }
        this.warning = warning;
        this.severe = severe;
        this.extreme = extreme;
    }

    public String getMetric() {
        return metric;
    }

    public double getWarning() {
        return warning;
    }

    public double getSevere() {
        return severe;
    }

    public double getExtreme() {
        return extreme;
    }

    /**
     * The most serious band the value breaches, or {@code null} when nothing is breached.
     * Checked from the top down so that a value crossing several bands raises one EXTREME alert
     * rather than three alerts of increasing severity.
     */
    public Severity highestBreachedBand(double value) {
        if (Double.isNaN(value)) {
            return null;
        }
        if (value >= extreme) {
            return Severity.EXTREME;
        }
        if (value >= severe) {
            return Severity.SEVERE;
        }
        if (value >= warning) {
            return Severity.WARNING;
        }
        return null;
    }

    /** The configured bound for a band, used to quote the breached number in the alert message. */
    public double thresholdFor(Severity severity) {
        if (severity == null) {
            throw new IllegalArgumentException("severity is required");
        }
        return switch (severity) {
            case EXTREME -> extreme;
            case SEVERE -> severe;
            case WARNING, INFO -> warning;
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ThresholdRule other)) {
            return false;
        }
        return Double.compare(warning, other.warning) == 0
                && Double.compare(severe, other.severe) == 0
                && Double.compare(extreme, other.extreme) == 0
                && metric.equals(other.metric);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metric, warning, severe, extreme);
    }

    @Override
    public String toString() {
        return "ThresholdRule{metric=" + metric
                + ", warning=" + warning
                + ", severe=" + severe
                + ", extreme=" + extreme
                + '}';
    }
}
