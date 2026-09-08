package com.stream.processing.common;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

/**
 * Derives an alert's identifier from the facts that produced it rather than from a random UUID.
 *
 * <p>Flink's Kafka sink gives at-least-once delivery unless the whole chain runs in exactly-once
 * mode, and a job restarted from a checkpoint will re-emit the windows it had already emitted.
 * Because the id is a UUIDv3 over (station, sensor type, window bounds, severity), a replayed
 * window yields the same id and the alert service's upsert collapses the duplicate instead of
 * raising the same flood warning twice.</p>
 */
public final class AlertIds {

    /** Namespace constant for the alert id space. */
    private static final String NAMESPACE = "stream.alert.v1";

    private AlertIds() {
        // static holder
    }

    public static String deterministicId(String stationId,
                                         SensorType sensorType,
                                         Instant windowStart,
                                         Instant windowEnd,
                                         Severity severity) {
        String seed = NAMESPACE
                + '|' + stationId
                + '|' + sensorType
                + '|' + (windowStart == null ? "" : windowStart.toEpochMilli())
                + '|' + (windowEnd == null ? "" : windowEnd.toEpochMilli())
                + '|' + severity;
        return UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)).toString();
    }
}
