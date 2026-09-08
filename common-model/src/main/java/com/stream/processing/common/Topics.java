package com.stream.processing.common;

/**
 * Kafka topic names shared by every stage of the pipeline.
 *
 * <p>The suffix carries the schema version so that a breaking change to an event
 * can be rolled out on a new topic while the old stage is still consuming.</p>
 */
public final class Topics {

    /** Raw readings emitted by the scrapers in the ingest service. Key: station id. */
    public static final String RAW_TELEMETRY = "telemetry.raw.v1";

    /** Per-station windowed aggregates emitted by the Flink aggregation stage. Key: station id. */
    public static final String AGGREGATED_TELEMETRY = "telemetry.aggregated.v1";

    /** Alerts emitted by the Flink threshold-evaluation stage. Key: station id. */
    public static final String GENERATED_ALERTS = "alerts.generated.v1";

    /** Records that could not be parsed or failed validation anywhere in the pipeline. */
    public static final String DEAD_LETTER = "telemetry.dlq.v1";

    private Topics() {
        // constants holder
    }
}
