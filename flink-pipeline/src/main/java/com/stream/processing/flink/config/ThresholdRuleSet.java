package com.stream.processing.flink.config;

import com.stream.processing.common.SensorType;

import java.io.Serializable;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/**
 * The complete threshold policy: one {@link ThresholdRule} per {@link SensorType}.
 *
 * <p>Shipped into the operators as a field rather than read from a static constant so that the
 * policy travels with the job graph. A savepoint restore or a re-submission with different
 * {@code --threshold.*} arguments then picks up the new bands without a code change, which is
 * what makes the numbers tunable during the monsoon rather than at compile time.</p>
 */
public final class ThresholdRuleSet implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<SensorType, ThresholdRule> rules;

    public ThresholdRuleSet(Map<SensorType, ThresholdRule> rules) {
        Objects.requireNonNull(rules, "rules");
        EnumMap<SensorType, ThresholdRule> copy = new EnumMap<>(SensorType.class);
        copy.putAll(rules);
        this.rules = Collections.unmodifiableMap(copy);
    }

    /** The rule for a sensor type, or {@code null} when that type has no configured policy. */
    public ThresholdRule ruleFor(SensorType sensorType) {
        return sensorType == null ? null : rules.get(sensorType);
    }

    public Map<SensorType, ThresholdRule> asMap() {
        return rules;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof ThresholdRuleSet other && rules.equals(other.rules);
    }

    @Override
    public int hashCode() {
        return rules.hashCode();
    }

    @Override
    public String toString() {
        return "ThresholdRuleSet" + rules;
    }
}
