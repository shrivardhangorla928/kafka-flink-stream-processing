package com.vassar.aware.alert.domain;

import com.vassar.aware.common.MeasurementUnit;
import com.vassar.aware.common.SensorType;
import com.vassar.aware.common.Severity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.Objects;

/**
 * Persistent form of {@link com.vassar.aware.common.Alert}.
 *
 * <p>A separate class on purpose: {@code Alert} is the wire contract shared with the ingest
 * service and the Flink job, and those modules must not drag {@code jakarta.persistence} onto
 * their classpath. It also lets the store carry fields the event has no business knowing about -
 * {@code receivedAt} and the acknowledgement trio.</p>
 *
 * <p>The identifier is assigned, never generated: it is the deterministic id derived from the
 * window, which is what makes the sink idempotent under Flink's at-least-once replay.</p>
 */
@Entity
@Table(name = "alerts")
public class AlertEntity {

    @Id
    @Column(name = "alert_id", nullable = false, length = 64)
    private String alertId;

    @Column(name = "station_id", nullable = false, length = 64)
    private String stationId;

    @Column(name = "station_name")
    private String stationName;

    @Column(name = "district_id", length = 64)
    private String districtId;

    @Enumerated(EnumType.STRING)
    @Column(name = "sensor_type", nullable = false, length = 32)
    private SensorType sensorType;

    @Enumerated(EnumType.STRING)
    @Column(name = "severity", nullable = false, length = 16)
    private Severity severity;

    /**
     * Denormalised {@link Severity#rank()}. Kept in the row rather than derived at query time so
     * that "WARNING and above" is an indexed integer range scan; it is written only by
     * {@link #setSeverity(Severity)} so it cannot drift from the enum.
     */
    @Column(name = "severity_rank", nullable = false)
    private int severityRank;

    @Column(name = "metric", length = 64)
    private String metric;

    @Column(name = "observed_value", nullable = false)
    private double observedValue;

    @Column(name = "threshold_value", nullable = false)
    private double thresholdValue;

    @Enumerated(EnumType.STRING)
    @Column(name = "unit", length = 16)
    private MeasurementUnit unit;

    @Column(name = "window_start")
    private Instant windowStart;

    @Column(name = "window_end")
    private Instant windowEnd;

    @Column(name = "message", length = 1024)
    private String message;

    @Column(name = "latitude", nullable = false)
    private double latitude;

    @Column(name = "longitude", nullable = false)
    private double longitude;

    @Column(name = "generated_at", nullable = false)
    private Instant generatedAt;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    @Column(name = "acknowledged", nullable = false)
    private boolean acknowledged;

    @Column(name = "acknowledged_at")
    private Instant acknowledgedAt;

    @Column(name = "acknowledged_by", length = 128)
    private String acknowledgedBy;

    protected AlertEntity() {
        // required by JPA
    }

    public AlertEntity(String alertId) {
        this.alertId = alertId;
    }

    public String getAlertId() {
        return alertId;
    }

    public void setAlertId(String alertId) {
        this.alertId = alertId;
    }

    public String getStationId() {
        return stationId;
    }

    public void setStationId(String stationId) {
        this.stationId = stationId;
    }

    public String getStationName() {
        return stationName;
    }

    public void setStationName(String stationName) {
        this.stationName = stationName;
    }

    public String getDistrictId() {
        return districtId;
    }

    public void setDistrictId(String districtId) {
        this.districtId = districtId;
    }

    public SensorType getSensorType() {
        return sensorType;
    }

    public void setSensorType(SensorType sensorType) {
        this.sensorType = sensorType;
    }

    public Severity getSeverity() {
        return severity;
    }

    /** Also refreshes {@link #severityRank}; there is no independent setter for the rank. */
    public void setSeverity(Severity severity) {
        this.severity = severity;
        this.severityRank = severity == null ? Severity.INFO.rank() : severity.rank();
    }

    public int getSeverityRank() {
        return severityRank;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public double getObservedValue() {
        return observedValue;
    }

    public void setObservedValue(double observedValue) {
        this.observedValue = observedValue;
    }

    public double getThresholdValue() {
        return thresholdValue;
    }

    public void setThresholdValue(double thresholdValue) {
        this.thresholdValue = thresholdValue;
    }

    public MeasurementUnit getUnit() {
        return unit;
    }

    public void setUnit(MeasurementUnit unit) {
        this.unit = unit;
    }

    public Instant getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(Instant windowStart) {
        this.windowStart = windowStart;
    }

    public Instant getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Instant windowEnd) {
        this.windowEnd = windowEnd;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public Instant getGeneratedAt() {
        return generatedAt;
    }

    public void setGeneratedAt(Instant generatedAt) {
        this.generatedAt = generatedAt;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }

    public void setReceivedAt(Instant receivedAt) {
        this.receivedAt = receivedAt;
    }

    public boolean isAcknowledged() {
        return acknowledged;
    }

    public void setAcknowledged(boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    public Instant getAcknowledgedAt() {
        return acknowledgedAt;
    }

    public void setAcknowledgedAt(Instant acknowledgedAt) {
        this.acknowledgedAt = acknowledgedAt;
    }

    public String getAcknowledgedBy() {
        return acknowledgedBy;
    }

    public void setAcknowledgedBy(String acknowledgedBy) {
        this.acknowledgedBy = acknowledgedBy;
    }

    /**
     * Records an acknowledgement, ignoring the call when one is already on file.
     *
     * <p>Idempotent by design: the dashboard retries the POST on a flaky connection, and the
     * first responder's name and timestamp are the audit trail - a retry must not overwrite them
     * with a later click.</p>
     *
     * @return {@code true} if this call is the one that acknowledged the alert
     */
    public boolean acknowledge(String by, Instant at) {
        if (acknowledged) {
            return false;
        }
        this.acknowledged = true;
        this.acknowledgedBy = by;
        this.acknowledgedAt = at;
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AlertEntity other)) {
            return false;
        }
        return alertId != null && alertId.equals(other.alertId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(alertId);
    }

    @Override
    public String toString() {
        return "AlertEntity{alertId=" + alertId
                + ", stationId=" + stationId
                + ", severity=" + severity
                + ", generatedAt=" + generatedAt
                + '}';
    }
}
