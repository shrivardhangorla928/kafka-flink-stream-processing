package com.stream.processing.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * An alert raised because a station's windowed aggregate breached a configured threshold,
 * published to {@link Topics#GENERATED_ALERTS} and persisted by the alert service.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Alert implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Deterministic identifier derived from station, sensor type and window bounds, so that a
     * replayed window produces the same id and the alert store can stay idempotent.
     */
    private String alertId;

    private String stationId;
    private String stationName;
    private String districtId;
    private SensorType sensorType;
    private Severity severity;

    /** Which quantity breached, e.g. {@code RAINFALL_WINDOW_SUM}. */
    private String metric;

    private double observedValue;
    private double thresholdValue;
    private MeasurementUnit unit;

    private Instant windowStart;
    private Instant windowEnd;

    private String message;

    private double latitude;
    private double longitude;

    private Instant generatedAt;

    public Alert() {
        // required by Jackson and by Flink's PojoSerializer
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

    public void setSeverity(Severity severity) {
        this.severity = severity;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Alert other)) {
            return false;
        }
        return Objects.equals(alertId, other.alertId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(alertId);
    }

    @Override
    public String toString() {
        return "Alert{alertId=" + alertId
                + ", stationId=" + stationId
                + ", severity=" + severity
                + ", observedValue=" + observedValue
                + ", thresholdValue=" + thresholdValue
                + '}';
    }
}
