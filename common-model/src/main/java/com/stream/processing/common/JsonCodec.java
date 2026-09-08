package com.stream.processing.common;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * Single JSON configuration used on every hop of the pipeline, so that a record written by the
 * ingest service, re-serialised by Flink and read by the alert service round-trips unchanged.
 *
 * <p>Instants are written as ISO-8601 strings rather than epoch decimals: the payloads stay
 * readable when inspecting a topic with {@code kafka-console-consumer} during evaluation.</p>
 */
public final class JsonCodec {

    private static final ObjectMapper MAPPER = create();

    private JsonCodec() {
        // static holder
    }

    /** A new mapper configured identically to the shared one, for frameworks that want to own it. */
    public static ObjectMapper create() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE);
    }

    public static ObjectMapper mapper() {
        return MAPPER;
    }

    public static byte[] toBytes(Object value) {
        try {
            return MAPPER.writeValueAsBytes(value);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to serialise " + value.getClass().getSimpleName(), e);
        }
    }

    public static String toJson(Object value) {
        return new String(toBytes(value), StandardCharsets.UTF_8);
    }

    public static <T> T fromBytes(byte[] payload, Class<T> type) {
        try {
            return MAPPER.readValue(payload, type);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to deserialise " + type.getSimpleName(), e);
        }
    }

    public static <T> T fromJson(String payload, Class<T> type) {
        return fromBytes(payload.getBytes(StandardCharsets.UTF_8), type);
    }
}
