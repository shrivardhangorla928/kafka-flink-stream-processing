package com.stream.processing.alert.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Every tunable number in the service, bound from {@code stream.alert.*} and validated at
 * startup so a bad value fails the pod's readiness probe instead of surfacing hours later as
 * an unbounded page request or a listener that silently never started.
 *
 * <p>Immutable constructor binding: the values are read on hot paths (every poll, every
 * request) and must not be mutable after the context has started.</p>
 *
 * @param consumeAggregates control over the secondary observability feed
 * @param api               limits applied to the query API
 * @param consumer          Kafka consumer tuning shared by both listeners
 * @param retry             error-handler backoff before a record is dead-lettered
 */
@Validated
@ConfigurationProperties(prefix = "stream.alert")
public record AlertServiceProperties(

        @Valid @NotNull @DefaultValue ConsumeAggregates consumeAggregates,
        @Valid @NotNull @DefaultValue Api api,
        @Valid @NotNull @DefaultValue Consumer consumer,
        @Valid @NotNull @DefaultValue Retry retry) {

    /**
     * The aggregate feed exists for observability, not for alerting. It is an order of magnitude
     * higher volume than the alert topic, so it must be switchable off independently when the
     * database is the constraint during a load test.
     *
     * @param enabled whether the {@code telemetry.aggregated.v1} listener starts
     */
    public record ConsumeAggregates(@DefaultValue("true") boolean enabled) {
    }

    /**
     * Query API limits.
     *
     * @param defaultPageSize page size applied when the caller does not ask for one
     * @param maxPageSize     hard cap; an unfiltered request must never be able to ask the
     *                        database for the whole alerts table
     * @param maxTimelineSize hard cap on the per-station aggregate timeline
     */
    public record Api(
            @Min(1) @Max(500) @DefaultValue("50") int defaultPageSize,
            @Min(1) @Max(1000) @DefaultValue("200") int maxPageSize,
            @Min(1) @Max(1000) @DefaultValue("200") int maxTimelineSize) {
    }

    /**
     * Kafka consumer tuning.
     *
     * @param concurrency    listener threads per topic; matched to the partition count so every
     *                       partition gets a thread and no thread sits idle
     * @param maxPollRecords upper bound on a batch, and therefore on one transaction's size
     * @param pollTimeout    how long a poll blocks before the container loops
     */
    public record Consumer(
            @Min(1) @Max(32) @DefaultValue("3") int concurrency,
            @Min(1) @Max(5000) @DefaultValue("500") int maxPollRecords,
            @NotNull @DefaultValue("3s") Duration pollTimeout) {
    }

    /**
     * Backoff applied by the {@code DefaultErrorHandler} before the offending record is routed
     * to the dead-letter topic. The total time spent retrying must stay comfortably below
     * {@code max.poll.interval.ms} (5 minutes) or the broker evicts the consumer from the group
     * mid-retry and the partition is rebalanced away.
     *
     * @param maxAttempts     retries after the first delivery
     * @param initialInterval first pause
     * @param multiplier      growth factor
     * @param maxInterval     ceiling on a single pause
     */
    public record Retry(
            @Min(0) @Max(20) @DefaultValue("4") int maxAttempts,
            @NotNull @DefaultValue("500ms") Duration initialInterval,
            @Positive @DefaultValue("2.0") double multiplier,
            @NotNull @DefaultValue("10s") Duration maxInterval) {
    }
}
