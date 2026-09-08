package com.stream.processing.flink.ingest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.stream.processing.common.Topics;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * A payload the pipeline refused, published to {@link Topics#DEAD_LETTER}.
 *
 * <p>The original bytes are carried verbatim as text so the record can be inspected, fixed and
 * replayed. Without that the only evidence of a scraper regression is a counter going up.</p>
 *
 * <p>A mutable POJO with a no-argument constructor for the same reason as the shared model
 * types: it lets Flink use the {@code PojoSerializer} rather than falling back to Kryo.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeadLetterRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The rejected payload, decoded as UTF-8. Truncated if absurdly large. */
    private String payload;

    /** Why it was rejected, e.g. a Jackson parse message or {@code failed isValid()}. */
    private String reason;

    /** Which stage rejected it, so a growing dead-letter topic points at a specific operator. */
    private String stage;

    private Instant failedAt;

    public DeadLetterRecord() {
        // required by Jackson and by Flink's PojoSerializer
    }

    public static DeadLetterRecord of(String payload, String reason, String stage, Instant failedAt) {
        DeadLetterRecord record = new DeadLetterRecord();
        record.setPayload(payload);
        record.setReason(reason);
        record.setStage(stage);
        record.setFailedAt(failedAt);
        return record;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public String getStage() {
        return stage;
    }

    public void setStage(String stage) {
        this.stage = stage;
    }

    public Instant getFailedAt() {
        return failedAt;
    }

    public void setFailedAt(Instant failedAt) {
        this.failedAt = failedAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DeadLetterRecord other)) {
            return false;
        }
        return Objects.equals(payload, other.payload)
                && Objects.equals(reason, other.reason)
                && Objects.equals(stage, other.stage)
                && Objects.equals(failedAt, other.failedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(payload, reason, stage, failedAt);
    }

    @Override
    public String toString() {
        return "DeadLetterRecord{stage=" + stage + ", reason=" + reason + '}';
    }
}
