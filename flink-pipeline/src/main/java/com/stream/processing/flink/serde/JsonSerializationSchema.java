package com.stream.processing.flink.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stream.processing.common.JsonCodec;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Writes any event to Kafka using the mapper configuration shared with the ingest and alert
 * services, so a record this job emits deserialises byte-for-byte identically downstream.
 *
 * <p>The mapper is {@code transient} and rebuilt on the task manager: {@link ObjectMapper} is not
 * Java-serializable, and holding it in a normal field would make the whole operator fail to ship —
 * the classic "task not serializable" failure.</p>
 *
 * @param <T> event type being written
 */
public final class JsonSerializationSchema<T> implements SerializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper mapper;

    @Override
    public void open(InitializationContext context) {
        this.mapper = JsonCodec.create();
    }

    @Override
    public byte[] serialize(T element) {
        if (element == null) {
            return new byte[0];
        }
        try {
            return mapper().writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to serialise " + element.getClass().getName(), e);
        }
    }

    /** Materialises the mapper on first use so the schema also works outside an operator, as in tests. */
    private ObjectMapper mapper() {
        if (mapper == null) {
            mapper = JsonCodec.create();
        }
        return mapper;
    }
}
