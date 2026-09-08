package com.stream.processing.flink.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stream.processing.common.JsonCodec;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.util.Objects;

/**
 * Reads an event from Kafka with the shared mapper configuration.
 *
 * <p>This schema is deliberately <em>not</em> used on the raw telemetry source. A schema that
 * throws on a malformed payload fails the task, and a poison record then loops forever through
 * the restart strategy. The raw source reads bytes and a downstream process function decides
 * between parsing and dead-lettering; this class exists for the paths where the payload is known
 * to be well-formed, and for symmetric round-trip testing of the wire format.</p>
 *
 * @param <T> event type being read
 */
public final class JsonDeserializationSchema<T> implements DeserializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> type;

    private transient ObjectMapper mapper;

    public JsonDeserializationSchema(Class<T> type) {
        this.type = Objects.requireNonNull(type, "type");
    }

    @Override
    public void open(InitializationContext context) {
        this.mapper = JsonCodec.create();
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null;
        }
        return mapper().readValue(message, type);
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(type);
    }

    /** Materialises the mapper on first use so the schema also works outside an operator, as in tests. */
    private ObjectMapper mapper() {
        if (mapper == null) {
            mapper = JsonCodec.create();
        }
        return mapper;
    }
}
