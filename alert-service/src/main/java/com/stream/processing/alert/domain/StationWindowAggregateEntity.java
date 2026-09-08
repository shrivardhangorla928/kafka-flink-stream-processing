package com.stream.processing.alert.domain;

import com.stream.processing.common.MeasurementUnit;
import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.Objects;

/**
 * Persistent form of {@link com.stream.processing.common.StationWindowAggregate}.
 *
 * <p>This table is the observability feed, not the alerting path: it exists so the dissertation
 * can plot what the windows actually computed next to which of them tripped a threshold, and so
 * a station's recent history is queryable without replaying Kafka.</p>
 */
@Entity
@Table(name = "station_window_aggregates")
public class StationWindowAggregateEntity {

    @EmbeddedId
    private StationWindowKey key;

    @Column(name = "window_end", nullable = false)
    private Instant windowEnd;

    @Column(name = "station_name")
    private String stationName;

    @Column(name = "district_id", length = 64)
    private String districtId;

    @Enumerated(EnumType.STRING)
    @Column(name = "unit", length = 16)
    private MeasurementUnit unit;

    @Column(name = "reading_count", nullable = false)
    private long readingCount;

    @Column(name = "sum_value", nullable = false)
    private double sum;

    @Column(name = "min_value", nullable = false)
    private double min;

    @Column(name = "max_value", nullable = false)
    private double max;

    @Column(name = "avg_value", nullable = false)
    private double avg;

    @Column(name = "aggregated_value", nullable = false)
    private double aggregatedValue;

    @Column(name = "latitude", nullable = false)
    private double latitude;

    @Column(name = "longitude", nullable = false)
    private double longitude;

    @Column(name = "computed_at")
    private Instant computedAt;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    protected StationWindowAggregateEntity() {
        // required by JPA
    }

    public StationWindowAggregateEntity(StationWindowKey key) {
        this.key = key;
    }

    public StationWindowKey getKey() {
        return key;
    }

    public void setKey(StationWindowKey key) {
        this.key = key;
    }

    public Instant getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Instant windowEnd) {
        this.windowEnd = windowEnd;
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

    public MeasurementUnit getUnit() {
        return unit;
    }

    public void setUnit(MeasurementUnit unit) {
        this.unit = unit;
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

    public Instant getReceivedAt() {
        return receivedAt;
    }

    public void setReceivedAt(Instant receivedAt) {
        this.receivedAt = receivedAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StationWindowAggregateEntity other)) {
            return false;
        }
        return key != null && key.equals(other.key);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key);
    }

    @Override
    public String toString() {
        return "StationWindowAggregateEntity{key=" + key
                + ", aggregatedValue=" + aggregatedValue
                + ", readingCount=" + readingCount
                + '}';
    }
}
