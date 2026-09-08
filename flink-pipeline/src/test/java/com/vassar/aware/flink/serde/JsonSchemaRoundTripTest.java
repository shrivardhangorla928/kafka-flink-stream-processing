package com.vassar.aware.flink.serde;

import com.vassar.aware.common.Alert;
import com.vassar.aware.common.AlertIds;
import com.vassar.aware.common.JsonCodec;
import com.vassar.aware.common.MeasurementUnit;
import com.vassar.aware.common.SensorReading;
import com.vassar.aware.common.SensorType;
import com.vassar.aware.common.Severity;
import com.vassar.aware.common.StationWindowAggregate;
import com.vassar.aware.flink.ingest.DeadLetterRecord;
import com.vassar.aware.flink.testing.TestEvents;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The wire format is a contract with two other services. These tests pin it to {@link JsonCodec}
 * byte for byte, so a mapper feature toggled here — date serialisation being the classic one —
 * cannot silently diverge from what the ingest and alert services read.
 */
@Timeout(30)
class JsonSchemaRoundTripTest {

    private static final Instant WINDOW_START = Instant.parse("2026-08-11T05:00:00Z");
    private static final Instant WINDOW_END = Instant.parse("2026-08-11T05:05:00Z");

    private static SensorReading reading() {
        return TestEvents.reading("STN-1", SensorType.RAINFALL, 12.5d, WINDOW_START.plusSeconds(60));
    }

    private static StationWindowAggregate aggregate() {
        return TestEvents.aggregate("STN-1", SensorType.RAINFALL, 31.5d, WINDOW_START, WINDOW_END);
    }

    private static Alert alert() {
        Alert alert = new Alert();
        alert.setAlertId(AlertIds.deterministicId(
                "STN-1", SensorType.RAINFALL, WINDOW_START, WINDOW_END, Severity.SEVERE));
        alert.setStationId("STN-1");
        alert.setStationName("STN-1 Gauge");
        alert.setDistrictId("D-STN-1");
        alert.setSensorType(SensorType.RAINFALL);
        alert.setSeverity(Severity.SEVERE);
        alert.setMetric("RAINFALL_WINDOW_SUM");
        alert.setObservedValue(31.5d);
        alert.setThresholdValue(30.0d);
        alert.setUnit(MeasurementUnit.MM);
        alert.setWindowStart(WINDOW_START);
        alert.setWindowEnd(WINDOW_END);
        alert.setMessage("SEVERE severity: STN-1 Gauge (STN-1) recorded rainfall of 31.50 mm.");
        alert.setLatitude(16.5d);
        alert.setLongitude(80.6d);
        alert.setGeneratedAt(WINDOW_END.plusSeconds(1));
        return alert;
    }

    private static <T> byte[] serialise(T value) {
        JsonSerializationSchema<T> schema = new JsonSerializationSchema<>();
        schema.open(null);
        return schema.serialize(value);
    }

    private static <T> T deserialise(byte[] payload, Class<T> type) throws IOException {
        JsonDeserializationSchema<T> schema = new JsonDeserializationSchema<>(type);
        schema.open(null);
        return schema.deserialize(payload);
    }

    @Test
    void sensorReadingIsWrittenExactlyAsJsonCodecWouldWriteIt() throws IOException {
        SensorReading original = reading();

        byte[] written = serialise(original);

        assertThat(written).isEqualTo(JsonCodec.toBytes(original));
        assertThat(deserialise(written, SensorReading.class)).isEqualTo(original);
    }

    @Test
    void stationWindowAggregateIsWrittenExactlyAsJsonCodecWouldWriteIt() throws IOException {
        StationWindowAggregate original = aggregate();

        byte[] written = serialise(original);

        assertThat(written).isEqualTo(JsonCodec.toBytes(original));
        StationWindowAggregate readBack = deserialise(written, StationWindowAggregate.class);
        assertThat(readBack).isEqualTo(original);
        assertThat(readBack.getComputedAt()).isEqualTo(original.getComputedAt());
        assertThat(readBack.getUnit()).isEqualTo(original.getUnit());
    }

    @Test
    void alertIsWrittenExactlyAsJsonCodecWouldWriteIt() throws IOException {
        Alert original = alert();

        byte[] written = serialise(original);

        assertThat(written).isEqualTo(JsonCodec.toBytes(original));
        Alert readBack = deserialise(written, Alert.class);
        // Alert equality is by id alone, so the payload fields are checked explicitly.
        assertThat(readBack.getAlertId()).isEqualTo(original.getAlertId());
        assertThat(readBack.getSeverity()).isEqualTo(original.getSeverity());
        assertThat(readBack.getObservedValue()).isEqualTo(original.getObservedValue());
        assertThat(readBack.getThresholdValue()).isEqualTo(original.getThresholdValue());
        assertThat(readBack.getUnit()).isEqualTo(original.getUnit());
        assertThat(readBack.getWindowStart()).isEqualTo(original.getWindowStart());
        assertThat(readBack.getWindowEnd()).isEqualTo(original.getWindowEnd());
        assertThat(readBack.getGeneratedAt()).isEqualTo(original.getGeneratedAt());
        assertThat(readBack.getMessage()).isEqualTo(original.getMessage());
    }

    @Test
    void deadLetterRecordRoundTrips() throws IOException {
        DeadLetterRecord original = DeadLetterRecord.of(
                "{\"broken\":", "unparseable JSON", "parse-sensor-reading", WINDOW_END);

        byte[] written = serialise(original);

        assertThat(written).isEqualTo(JsonCodec.toBytes(original));
        assertThat(deserialise(written, DeadLetterRecord.class)).isEqualTo(original);
    }

    @Test
    void instantsAreWrittenAsIso8601StringsNotEpochDecimals() {
        String json = new String(serialise(reading()), StandardCharsets.UTF_8);

        assertThat(json).contains("\"eventTime\":\"2026-08-11T05:01:00Z\"");
    }

    @Test
    void schemasWorkWithoutOpenBeingCalled() throws IOException {
        // The mini cluster and the Kafka connector both call open(), but a schema used directly
        // must not depend on that having happened.
        SensorReading original = reading();

        byte[] written = new JsonSerializationSchema<SensorReading>().serialize(original);
        SensorReading readBack = new JsonDeserializationSchema<>(SensorReading.class).deserialize(written);

        assertThat(written).isEqualTo(JsonCodec.toBytes(original));
        assertThat(readBack).isEqualTo(original);
    }

    @Test
    void unknownFieldsFromANewerProducerAreIgnored() throws IOException {
        String futureSchema = "{\"stationId\":\"STN-9\",\"sensorType\":\"RAINFALL\",\"value\":3.0,"
                + "\"eventTime\":\"2026-08-11T05:01:00Z\",\"telemetryQualityFlag\":\"GOOD\"}";

        SensorReading readBack = deserialise(TestEvents.bytes(futureSchema), SensorReading.class);

        assertThat(readBack).isNotNull();
        assertThat(readBack.getStationId()).isEqualTo("STN-9");
        assertThat(readBack.isValid()).isTrue();
    }

    @Test
    void nullAndEmptyPayloadsAreHandledWithoutThrowing() throws IOException {
        assertThat(serialise(null)).isEmpty();
        assertThat(deserialise(new byte[0], SensorReading.class)).isNull();
        assertThat(deserialise(null, SensorReading.class)).isNull();
    }

    @Test
    void rawPayloadSchemaHandsBytesThroughUnchanged() {
        RawPayloadDeserializationSchema schema = new RawPayloadDeserializationSchema();
        byte[] payload = JsonCodec.toBytes(reading());

        assertThat(schema.deserialize(payload)).isSameAs(payload);
        assertThat(schema.isEndOfStream(payload)).isFalse();
        assertThat(schema.getProducedType().getTypeClass()).isEqualTo(byte[].class);
    }

    @Test
    void aTombstoneBecomesAnEmptyPayloadSoItStillReachesTheDeadLetterTopic() {
        assertThat(new RawPayloadDeserializationSchema().deserialize(null)).isEmpty();
    }

    @Test
    void messageKeysAreTheStationIdInUtf8() {
        StringKeySerializationSchema<Alert> keys = new StringKeySerializationSchema<>(Alert::getStationId);

        assertThat(keys.serialize(alert())).isEqualTo("STN-1".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void aNullKeyIsPassedThroughSoKafkaRoundRobinsTheRecord() {
        StringKeySerializationSchema<DeadLetterRecord> keys =
                new StringKeySerializationSchema<>(record -> null);

        assertThat(keys.serialize(DeadLetterRecord.of("x", "y", "z", WINDOW_END))).isNull();
        assertThat(keys.serialize(null)).isNull();
    }

    @Test
    void producedTypeIsReportedSoFlinkDoesNotFallBackToKryo() {
        assertThat(new JsonDeserializationSchema<>(Alert.class).getProducedType().getTypeClass())
                .isEqualTo(Alert.class);
        assertThat(new JsonDeserializationSchema<>(Alert.class).isEndOfStream(alert())).isFalse();
    }
}
