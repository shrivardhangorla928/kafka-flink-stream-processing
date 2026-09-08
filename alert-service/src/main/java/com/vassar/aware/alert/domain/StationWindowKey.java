package com.vassar.aware.alert.domain;

import com.vassar.aware.common.SensorType;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * Natural key of a windowed aggregate: the station, what it measured and which window.
 *
 * <p>Using the natural key as the primary key rather than a surrogate id is what makes a Flink
 * restart harmless - the re-emitted window collides with the row it already wrote and JPA turns
 * the save into an update instead of appending a second copy of the same minute of history.</p>
 */
@Embeddable
public class StationWindowKey implements Serializable {

    private static final long serialVersionUID = 1L;

    @Column(name = "station_id", nullable = false, length = 64)
    private String stationId;

    @Enumerated(EnumType.STRING)
    @Column(name = "sensor_type", nullable = false, length = 32)
    private SensorType sensorType;

    @Column(name = "window_start", nullable = false)
    private Instant windowStart;

    protected StationWindowKey() {
        // required by JPA
    }

    public StationWindowKey(String stationId, SensorType sensorType, Instant windowStart) {
        this.stationId = stationId;
        this.sensorType = sensorType;
        this.windowStart = windowStart;
    }

    public String getStationId() {
        return stationId;
    }

    public SensorType getSensorType() {
        return sensorType;
    }

    public Instant getWindowStart() {
        return windowStart;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StationWindowKey other)) {
            return false;
        }
        return Objects.equals(stationId, other.stationId)
                && sensorType == other.sensorType
                && Objects.equals(windowStart, other.windowStart);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stationId, sensorType, windowStart);
    }

    @Override
    public String toString() {
        return stationId + '/' + sensorType + '@' + windowStart;
    }
}
