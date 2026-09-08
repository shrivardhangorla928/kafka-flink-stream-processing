package com.stream.processing.flink.serde;

import org.apache.flink.api.common.serialization.SerializationSchema;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Serialises the Kafka message key as UTF-8 text taken from the event itself.
 *
 * <p>Every downstream topic is keyed by station id so that all events for one station land on
 * one partition and stay in order relative to each other, which is what lets the alert service
 * reason about a station's history without a global sort.</p>
 *
 * @param <T> event type the key is taken from
 */
public final class StringKeySerializationSchema<T> implements SerializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private final KeyExtractor<T> extractor;

    public StringKeySerializationSchema(KeyExtractor<T> extractor) {
        this.extractor = Objects.requireNonNull(extractor, "extractor");
    }

    @Override
    public byte[] serialize(T element) {
        String key = element == null ? null : extractor.keyOf(element);
        // A null key is legitimate: Kafka then round-robins the record across partitions, which
        // is the right behaviour for dead letters whose station could not be determined.
        return key == null ? null : key.getBytes(StandardCharsets.UTF_8);
    }
}
