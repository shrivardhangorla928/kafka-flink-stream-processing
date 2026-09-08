package com.vassar.aware.flink.ingest;

import com.vassar.aware.common.JsonCodec;
import com.vassar.aware.common.SensorReading;
import com.vassar.aware.common.SensorType;
import com.vassar.aware.flink.testing.TestEvents;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The parse stage is the pipeline's blast door. These tests pin the property that matters: a bad
 * payload is diverted, never thrown, because a throw here fails the task, the restart strategy
 * replays the same offset, and the job livelocks on one poison record.
 */
@Timeout(60)
class ParseSensorReadingFunctionTest {

    private static final Instant T0 = Instant.parse("2026-08-11T05:00:00Z");

    private OneInputStreamOperatorTestHarness<byte[], SensorReading> harness;

    @BeforeEach
    void setUp() throws Exception {
        harness = ProcessFunctionTestHarnesses.forProcessFunction(new ParseSensorReadingFunction());
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    private List<DeadLetterRecord> deadLetters() {
        // The harness returns null, not an empty queue, until the tag has been written to at least
        // once — so a run that dead-letters nothing has to be normalised here.
        ConcurrentLinkedQueue<StreamRecord<DeadLetterRecord>> sideOutput =
                harness.getSideOutput(ParseSensorReadingFunction.DEAD_LETTER_TAG);
        return sideOutput == null
                ? List.of()
                : sideOutput.stream().map(StreamRecord::getValue).collect(Collectors.toList());
    }

    @Test
    void parsesAWellFormedReading() throws Exception {
        SensorReading original = TestEvents.reading("STN-1", SensorType.RAINFALL, 12.5d, T0);

        harness.processElement(JsonCodec.toBytes(original), 0L);

        assertThat(harness.extractOutputValues()).containsExactly(original);
        assertThat(deadLetters()).isEmpty();
    }

    @Test
    void divertsMalformedJsonInsteadOfKillingTheJob() throws Exception {
        harness.processElement(TestEvents.bytes("{\"stationId\":\"STN-1\",  <-- not json"), 0L);

        assertThat(harness.extractOutputValues()).isEmpty();
        assertThat(deadLetters()).hasSize(1);
        DeadLetterRecord record = deadLetters().get(0);
        assertThat(record.getReason()).startsWith("unparseable JSON");
        assertThat(record.getStage()).isEqualTo(ParseSensorReadingFunction.STAGE);
        assertThat(record.getPayload()).contains("not json");
        assertThat(record.getFailedAt()).isNotNull();
    }

    @Test
    void keepsProcessingAfterAPoisonRecord() throws Exception {
        SensorReading before = TestEvents.reading("STN-1", SensorType.RAINFALL, 1.0d, T0);
        SensorReading after = TestEvents.reading("STN-2", SensorType.RIVER_LEVEL, 2.0d, T0.plusSeconds(60));

        harness.processElement(JsonCodec.toBytes(before), 0L);
        harness.processElement(TestEvents.bytes("}}}garbage{{{"), 1L);
        harness.processElement(JsonCodec.toBytes(after), 2L);

        assertThat(harness.extractOutputValues()).containsExactly(before, after);
        assertThat(deadLetters()).hasSize(1);
    }

    @Test
    void divertsStructurallyValidJsonThatFailsValidation() throws Exception {
        // Parses cleanly but has no station id, so it could never be keyed or windowed.
        String noStation = "{\"sensorType\":\"RAINFALL\",\"value\":3.0,\"eventTime\":\"2026-08-11T05:01:00Z\"}";

        harness.processElement(TestEvents.bytes(noStation), 0L);

        assertThat(harness.extractOutputValues()).isEmpty();
        assertThat(deadLetters()).singleElement()
                .satisfies(record -> assertThat(record.getReason()).isEqualTo("failed SensorReading.isValid()"));
    }

    @Test
    void divertsAReadingWithoutAnEventTimeBecauseItCannotBeWindowed() throws Exception {
        SensorReading timeless = TestEvents.reading("STN-1", SensorType.RAINFALL, 3.0d, T0);
        timeless.setEventTime(null);

        harness.processElement(JsonCodec.toBytes(timeless), 0L);

        assertThat(harness.extractOutputValues()).isEmpty();
        assertThat(deadLetters()).hasSize(1);
    }

    @Test
    void divertsAPhysicallyImpossibleNegativeReading() throws Exception {
        SensorReading negative = TestEvents.reading("STN-1", SensorType.RAINFALL, -4.0d, T0);

        harness.processElement(JsonCodec.toBytes(negative), 0L);

        assertThat(harness.extractOutputValues()).isEmpty();
        assertThat(deadLetters()).hasSize(1);
    }

    @Test
    void divertsAnEmptyPayload() throws Exception {
        harness.processElement(new byte[0], 0L);

        assertThat(harness.extractOutputValues()).isEmpty();
        assertThat(deadLetters()).singleElement()
                .satisfies(record -> assertThat(record.getReason()).isEqualTo("empty payload"));
    }

    @Test
    void divertsAJsonNullLiteral() throws Exception {
        harness.processElement(TestEvents.bytes("null"), 0L);

        assertThat(harness.extractOutputValues()).isEmpty();
        assertThat(deadLetters()).singleElement()
                .satisfies(record -> assertThat(record.getReason()).isEqualTo("payload deserialised to null"));
    }

    @Test
    void truncatesAnOversizedPayloadSoOneBlobCannotFloodTheDeadLetterTopic() throws Exception {
        harness.processElement(TestEvents.bytes("{" + "x".repeat(20_000)), 0L);

        String payload = deadLetters().get(0).getPayload();
        assertThat(payload).endsWith("...[truncated]");
        assertThat(payload.length()).isLessThan(5_000);
    }

    @Test
    void countsWhatItConsumedAndWhatItRejected() throws Exception {
        harness.processElement(JsonCodec.toBytes(TestEvents.reading("STN-1", SensorType.RAINFALL, 1.0d, T0)), 0L);
        harness.processElement(TestEvents.bytes("nonsense"), 1L);
        harness.processElement(TestEvents.bytes(""), 2L);

        assertThat(harness.extractOutputValues()).hasSize(1);
        assertThat(deadLetters()).hasSize(2);
    }
}
