package com.stream.processing.alert.service;

import com.stream.processing.common.Alert;
import com.stream.processing.common.SensorType;
import com.stream.processing.common.Severity;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * The service's instrumentation, kept in one class so the metric names, tags and units are
 * defined once and cannot drift between the consumer and the tests.
 *
 * <p>Meters are registered with Micrometer's dot-separated naming; the Prometheus registry
 * renames them to snake_case and appends {@code _total} to counters, so the names quoted in the
 * dissertation's Grafana dashboards are the ones in the constants below with dots replaced by
 * underscores - e.g. {@code stream.alerts.consumed} is scraped as
 * {@code stream_alerts_consumed_total}.</p>
 */
@Component
public class AlertMetrics {

    /** Newly stored alerts. Scraped as {@code stream_alerts_consumed_total}. */
    public static final String CONSUMED = "stream.alerts.consumed";

    /** Replays collapsed by the idempotent upsert. Scraped as {@code stream_alerts_duplicate_total}. */
    public static final String DUPLICATE = "stream.alerts.duplicate";

    /** Batches that failed to persist. Scraped as {@code stream_alerts_persist_failures_total}. */
    public static final String PERSIST_FAILURES = "stream.alerts.persist.failures";

    /** Time to commit one batch. Scraped as {@code stream_alerts_persist_latency_seconds}. */
    public static final String PERSIST_LATENCY = "stream.alerts.persist.latency";

    /**
     * Window close to sink arrival. Scraped as {@code stream_alerts_pipeline_latency_seconds}.
     *
     * <p>This is the headline number of the evaluation chapter: it spans Flink's window firing,
     * the threshold stage, the Kafka hop and this service's transaction, which is the whole
     * "how fresh is an alert" question the platform exists to answer.</p>
     */
    public static final String PIPELINE_LATENCY = "stream.alerts.pipeline.latency";

    /** Aggregate rows written by the observability feed. */
    public static final String AGGREGATES_CONSUMED = "stream.aggregates.consumed";

    /** Records routed to the dead-letter topic because they could not be parsed. */
    public static final String DEAD_LETTERED = "stream.alerts.dead.lettered";

    private static final String TAG_SEVERITY = "severity";
    private static final String TAG_SENSOR_TYPE = "sensor_type";

    private final MeterRegistry registry;
    private final Counter duplicates;
    private final Counter persistFailures;
    private final Counter deadLettered;
    private final Counter aggregatesConsumed;
    private final Timer persistLatency;
    private final Timer pipelineLatency;

    public AlertMetrics(MeterRegistry registry) {
        this.registry = registry;

        this.duplicates = Counter.builder(DUPLICATE)
                .description("Alerts discarded as replays of an already stored alertId")
                .register(registry);
        this.persistFailures = Counter.builder(PERSIST_FAILURES)
                .description("Batches whose transaction failed, leaving the offsets uncommitted")
                .register(registry);
        this.deadLettered = Counter.builder(DEAD_LETTERED)
                .description("Records published to the dead-letter topic after retries were exhausted")
                .register(registry);
        this.aggregatesConsumed = Counter.builder(AGGREGATES_CONSUMED)
                .description("Windowed aggregates upserted from the observability feed")
                .register(registry);

        this.persistLatency = Timer.builder(PERSIST_LATENCY)
                .description("Time to persist and commit one consumed batch")
                .publishPercentiles(0.5d, 0.95d, 0.99d)
                .register(registry);

        // Percentile histogram rather than plain percentiles: the evaluation chapter compares
        // latency distributions across load levels, and only a histogram can be aggregated
        // across replicas in Prometheus without lying about the quantiles.
        this.pipelineLatency = Timer.builder(PIPELINE_LATENCY)
                .description("Event-time window close to arrival in the alert store")
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(50))
                .maximumExpectedValue(Duration.ofMinutes(5))
                .register(registry);
    }

    /**
     * Counts one newly stored alert, tagged so a Grafana panel can break the rate down by band
     * and sensor without a separate metric per combination.
     */
    public void alertConsumed(Severity severity, SensorType sensorType) {
        Counter.builder(CONSUMED)
                .description("Alerts newly persisted by this service")
                .tag(TAG_SEVERITY, severity == null ? "UNKNOWN" : severity.name())
                .tag(TAG_SENSOR_TYPE, sensorType == null ? "UNKNOWN" : sensorType.name())
                .register(registry)
                .increment();
    }

    public void duplicates(long count) {
        if (count > 0) {
            duplicates.increment(count);
        }
    }

    public void persistFailure() {
        persistFailures.increment();
    }

    public void deadLettered() {
        deadLettered.increment();
    }

    public void aggregatesConsumed(long count) {
        if (count > 0) {
            aggregatesConsumed.increment(count);
        }
    }

    public void persistLatency(Duration duration) {
        persistLatency.record(duration);
    }

    /**
     * Records the pipeline latency of one alert.
     *
     * <p>Only called for alerts that were actually new. A replayed window can be hours old, and
     * feeding that gap into the histogram would make the headline latency figure a function of
     * how often Flink was restarted rather than of how fast the pipeline is.</p>
     *
     * <p>Negative gaps - a window whose end is in the future because the producer's clock runs
     * ahead - are dropped rather than clamped to zero, which would quietly bias the median down.</p>
     */
    public void pipelineLatency(Alert alert, Instant receivedAt) {
        if (alert.getWindowEnd() == null || receivedAt == null) {
            return;
        }
        long millis = Duration.between(alert.getWindowEnd(), receivedAt).toMillis();
        if (millis >= 0) {
            pipelineLatency.record(millis, TimeUnit.MILLISECONDS);
        }
    }
}
