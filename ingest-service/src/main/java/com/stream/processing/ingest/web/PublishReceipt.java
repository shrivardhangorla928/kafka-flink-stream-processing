package com.stream.processing.ingest.web;

import java.time.Instant;

/**
 * Body of a 202 response: how many readings were handed to the producer, and when.
 *
 * <p>The endpoints answer "accepted", not "stored". Sends are asynchronous and durable by
 * configuration, so a caller that needs delivery confirmation should watch the
 * {@code stream_ingest_publish_failures_total} metric rather than expect it in this response.</p>
 *
 * @param accepted   number of readings handed to the Kafka producer
 * @param topic      topic the readings were routed to when valid
 * @param acceptedAt server time the request was accepted
 */
public record PublishReceipt(int accepted, String topic, Instant acceptedAt) {

    public static PublishReceipt of(int accepted, String topic) {
        return new PublishReceipt(accepted, topic, Instant.now());
    }
}
