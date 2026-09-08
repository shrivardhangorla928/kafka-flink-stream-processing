package com.vassar.aware.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * A single raw observation scraped from a station, published to {@link Topics#RAW_TELEMETRY}.
 *
 * <p>Deliberately a mutable POJO with a public no-argument constructor: that is what lets Flink
 * use its efficient {@code PojoSerializer} for network shuffles and state instead of falling
 * back to Kryo.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SensorReading implements Serializable {

    private static final long serialVersionUID = 1L;

    private String readingId;
    private String stationId;
    private String stationName;
    private String districtId;
    private SensorType sensorType;
    private double value;
    private MeasurementUnit unit;
    private double latitude;
    private double longitude;

    /** Time the observation was taken at the station. This is the event time Flink windows on. */
    private Instant eventTime;

    /** Time the scraper published it. eventTime to ingestedAt is the scrape lag. */
    private Instant ingestedAt;

    /** Identifier of the scraper that produced the record, e.g. {@code IMD_SCRAPER}. */
    private String source;

    public SensorReading() {
        // required by Jackson and by Flink's PojoSerializer
    }

    public String getReadingId() {
        return readingId;
    }

    public void setReadingId(String readingId) {
        this.readingId = readingId;
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

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public MeasurementUnit getUnit() {
        return unit;
    }

    public void setUnit(MeasurementUnit unit) {
        this.unit = unit;
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

    public Instant getEventTime() {
        return eventTime;
    }

    public void setEventTime(Instant eventTime) {
        this.eventTime = eventTime;
    }

    public Instant getIngestedAt() {
        return ingestedAt;
    }

    public void setIngestedAt(Instant ingestedAt) {
        this.ingestedAt = ingestedAt;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    /**
     * A reading is usable by the pipeline only if it can be keyed, windowed and measured.
     * Records failing this check are routed to {@link Topics#DEAD_LETTER}.
     */
    public boolean isValid() {
        return stationId != null && !stationId.isBlank()
                && sensorType != null
                && eventTime != null
                && !Double.isNaN(value)
                && !Double.isInfinite(value)
                && value >= 0.0d;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SensorReading other)) {
            return false;
        }
        return Double.compare(value, other.value) == 0
                && Double.compare(latitude, other.latitude) == 0
                && Double.compare(longitude, other.longitude) == 0
                && Objects.equals(readingId, other.readingId)
                && Objects.equals(stationId, other.stationId)
                && Objects.equals(stationName, other.stationName)
                && Objects.equals(districtId, other.districtId)
                && sensorType == other.sensorType
                && unit == other.unit
                && Objects.equals(eventTime, other.eventTime)
                && Objects.equals(ingestedAt, other.ingestedAt)
                && Objects.equals(source, other.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(readingId, stationId, stationName, districtId, sensorType, value, unit,
                latitude, longitude, eventTime, ingestedAt, source);
    }

    @Override
    public String toString() {
        return "SensorReading{stationId=" + stationId
                + ", sensorType=" + sensorType
                + ", value=" + value
                + ", eventTime=" + eventTime
                + '}';
    }
}
