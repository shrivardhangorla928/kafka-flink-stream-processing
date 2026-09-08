package com.stream.processing.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * The result of collapsing every reading a station produced inside one tumbling event-time
 * window, published to {@link Topics#AGGREGATED_TELEMETRY}.
 *
 * <p>{@link #getAggregatedValue()} is the single number the threshold stage evaluates; which of
 * sum/max/avg it holds is decided by {@link SensorType#getAggregationKind()}.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StationWindowAggregate implements Serializable {

    private static final long serialVersionUID = 1L;

    private String stationId;
    private String stationName;
    private String districtId;
    private SensorType sensorType;
    private MeasurementUnit unit;

    private Instant windowStart;
    private Instant windowEnd;

    private long readingCount;
    private double sum;
    private double min;
    private double max;
    private double avg;

    /** The value the thresholds are applied to, selected by the sensor type's aggregation kind. */
    private double aggregatedValue;

    private double latitude;
    private double longitude;

    /** Wall-clock time the window fired, used to measure end-to-end pipeline latency. */
    private Instant computedAt;

    public StationWindowAggregate() {
        // required by Jackson and by Flink's PojoSerializer
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

    public long getReadingCount() {
        return readingCount;
    }

    public void setReadingCount(long readingCount) {
        this.readingCount = readingCount;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public double getAvg() {
        return avg;
    }

    public void setAvg(double avg) {
        this.avg = avg;
    }

    public double getAggregatedValue() {
        return aggregatedValue;
    }

    public void setAggregatedValue(double aggregatedValue) {
        this.aggregatedValue = aggregatedValue;
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

    public Instant getComputedAt() {
        return computedAt;
    }

    public void setComputedAt(Instant computedAt) {
        this.computedAt = computedAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StationWindowAggregate other)) {
            return false;
        }
        return readingCount == other.readingCount
                && Double.compare(sum, other.sum) == 0
                && Double.compare(min, other.min) == 0
                && Double.compare(max, other.max) == 0
                && Double.compare(avg, other.avg) == 0
                && Double.compare(aggregatedValue, other.aggregatedValue) == 0
                && Objects.equals(stationId, other.stationId)
                && sensorType == other.sensorType
                && Objects.equals(windowStart, other.windowStart)
                && Objects.equals(windowEnd, other.windowEnd);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stationId, sensorType, windowStart, windowEnd, readingCount, sum, min, max,
                avg, aggregatedValue);
    }

    @Override
    public String toString() {
        return "StationWindowAggregate{stationId=" + stationId
                + ", sensorType=" + sensorType
                + ", window=[" + windowStart + ", " + windowEnd + ")"
                + ", readingCount=" + readingCount
                + ", aggregatedValue=" + aggregatedValue
                + '}';
    }
}
