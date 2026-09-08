package com.stream.processing.flink.serde;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Hands the Kafka record value through as raw bytes.
 *
 * <p>The raw telemetry source has to tolerate garbage. Any schema that parses inside the source
 * can only signal failure by throwing, which fails the task; the restart strategy then replays
 * the same offset and the job livelocks on one poison record. Reading bytes here moves the
 * decision one operator downstream, where a bad payload can be side-outputted to the dead-letter
 * topic instead of taking the pipeline down.</p>
 *
 * <p>A {@code null} value (a Kafka tombstone) is turned into an empty array rather than dropped,
 * so that it still shows up in the dead-letter topic and is visible during evaluation.</p>
 */
public final class RawPayloadDeserializationSchema implements DeserializationSchema<byte[]> {

    private static final long serialVersionUID = 1L;

    private static final byte[] EMPTY = new byte[0];

    @Override
    public byte[] deserialize(byte[] message) {
        return message == null ? EMPTY : message;
    }

    @Override
    public boolean isEndOfStream(byte[] nextElement) {
        return false;
    }

    @Override
    public TypeInformation<byte[]> getProducedType() {
        return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
    }
}
