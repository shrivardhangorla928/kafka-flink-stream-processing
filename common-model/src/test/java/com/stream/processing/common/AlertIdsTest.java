package com.stream.processing.common;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The alert id is the pipeline's idempotency key. Its whole value comes from being a pure
 * function of the facts that produced the alert, so these tests guard exactly that property.
 */
class AlertIdsTest {

    private static final Instant WINDOW_START = Instant.parse("2026-08-11T10:15:00Z");
    private static final Instant WINDOW_END = WINDOW_START.plusSeconds(300);

    @Test
    @DisplayName("the same window replayed after a checkpoint restore yields the same id")
    void isDeterministic() {
        String first = AlertIds.deterministicId("STN-0007", SensorType.RAINFALL,
                WINDOW_START, WINDOW_END, Severity.SEVERE);
        String second = AlertIds.deterministicId("STN-0007", SensorType.RAINFALL,
                WINDOW_START, WINDOW_END, Severity.SEVERE);

        assertThat(first).isEqualTo(second);
    }

    @Test
    @DisplayName("the id is a well-formed UUID")
    void isAWellFormedUuid() {
        String id = AlertIds.deterministicId("STN-0007", SensorType.RAINFALL,
                WINDOW_START, WINDOW_END, Severity.WARNING);

        assertThat(UUID.fromString(id)).hasToString(id);
    }

    @Test
    @DisplayName("a different station, sensor, window or severity produces a different id")
    void discriminatesOnEveryComponent() {
        String base = AlertIds.deterministicId("STN-0007", SensorType.RAINFALL,
                WINDOW_START, WINDOW_END, Severity.SEVERE);

        assertThat(base)
                .isNotEqualTo(AlertIds.deterministicId("STN-0008", SensorType.RAINFALL,
                        WINDOW_START, WINDOW_END, Severity.SEVERE))
                .isNotEqualTo(AlertIds.deterministicId("STN-0007", SensorType.RIVER_LEVEL,
                        WINDOW_START, WINDOW_END, Severity.SEVERE))
                .isNotEqualTo(AlertIds.deterministicId("STN-0007", SensorType.RAINFALL,
                        WINDOW_START.plusSeconds(300), WINDOW_END.plusSeconds(300), Severity.SEVERE))
                .isNotEqualTo(AlertIds.deterministicId("STN-0007", SensorType.RAINFALL,
                        WINDOW_START, WINDOW_END, Severity.EXTREME));
    }

    @Test
    @DisplayName("the delimiter prevents adjacent fields from running together into a collision")
    void doesNotCollideOnConcatenatedFields() {
        String a = AlertIds.deterministicId("STN-1", SensorType.RAINFALL,
                WINDOW_START, WINDOW_END, Severity.WARNING);
        String b = AlertIds.deterministicId("STN", SensorType.RAINFALL,
                WINDOW_START, WINDOW_END, Severity.WARNING);

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    @DisplayName("null window bounds are tolerated rather than throwing inside an operator")
    void toleratesNullWindowBounds() {
        String id = AlertIds.deterministicId("STN-0007", SensorType.RAINFALL, null, null, Severity.INFO);

        assertThat(UUID.fromString(id)).hasToString(id);
        assertThat(id).isEqualTo(
                AlertIds.deterministicId("STN-0007", SensorType.RAINFALL, null, null, Severity.INFO));
    }
}
