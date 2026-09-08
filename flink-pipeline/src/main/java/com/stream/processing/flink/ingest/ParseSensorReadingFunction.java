package com.stream.processing.flink.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stream.processing.common.JsonCodec;
import com.stream.processing.common.SensorReading;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Turns raw Kafka payloads into {@link SensorReading}s, diverting anything unusable to a side
 * output instead of failing.
 *
 * <p>This is the only place in the job that is allowed to see a malformed record. A scraper that
 * starts emitting broken JSON at 3am must not be able to stop flood alerts for every other
 * station, so parse failures and records failing {@link SensorReading#isValid()} are counted and
 * side-outputted rather than thrown.</p>
 *
 * <p>Top-level and stateless apart from transient runtime handles, which is what keeps it
 * serializable — an anonymous or inner class here would drag the enclosing job class into the
 * closure and fail to ship.</p>
 */
public final class ParseSensorReadingFunction extends ProcessFunction<byte[], SensorReading> {

    /** Side output carrying everything this stage refused. */
    public static final OutputTag<DeadLetterRecord> DEAD_LETTER_TAG =
            new OutputTag<>("dead-letter", TypeInformation.of(DeadLetterRecord.class));

    /** Stage name stamped on dead letters produced here. */
    public static final String STAGE = "parse-sensor-reading";

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ParseSensorReadingFunction.class);

    /** Cap on the payload copied into a dead letter, so one huge blob cannot flood the DLQ topic. */
    private static final int MAX_PAYLOAD_CHARS = 4096;

    /** Logging a whole bad payload per record would be its own outage; sample instead. */
    private static final long LOG_EVERY_N_FAILURES = 100L;

    private transient ObjectMapper mapper;
    private transient Counter recordsConsumed;
    private transient Counter recordsDeadLettered;
    private transient long failureCount;

    @Override
    public void open(OpenContext openContext) {
        this.mapper = JsonCodec.create();
        this.recordsConsumed = getRuntimeContext().getMetricGroup().counter("recordsConsumed");
        this.recordsDeadLettered = getRuntimeContext().getMetricGroup().counter("recordsDeadLettered");
        this.failureCount = 0L;
    }

    @Override
    public void processElement(byte[] payload, Context ctx, Collector<SensorReading> out) {
        recordsConsumed.inc();

        if (payload == null || payload.length == 0) {
            deadLetter(ctx, "", "empty payload");
            return;
        }

        SensorReading reading;
        try {
            reading = mapper.readValue(payload, SensorReading.class);
        } catch (Exception e) {
            deadLetter(ctx, text(payload), "unparseable JSON: " + e.getMessage());
            return;
        }

        if (reading == null) {
            deadLetter(ctx, text(payload), "payload deserialised to null");
            return;
        }
        if (!reading.isValid()) {
            deadLetter(ctx, text(payload), "failed SensorReading.isValid()");
            return;
        }

        out.collect(reading);
    }

    private void deadLetter(Context ctx, String payload, String reason) {
        recordsDeadLettered.inc();
        failureCount++;
        if (failureCount == 1L || failureCount % LOG_EVERY_N_FAILURES == 0L) {
            LOG.warn("Dead-lettering telemetry payload ({} so far in this subtask): {}", failureCount, reason);
        }
        ctx.output(DEAD_LETTER_TAG, DeadLetterRecord.of(truncate(payload), reason, STAGE, Instant.now()));
    }

    private static String text(byte[] payload) {
        return new String(payload, StandardCharsets.UTF_8);
    }

    private static String truncate(String payload) {
        if (payload == null || payload.length() <= MAX_PAYLOAD_CHARS) {
            return payload;
        }
        return payload.substring(0, MAX_PAYLOAD_CHARS) + "...[truncated]";
    }
}
