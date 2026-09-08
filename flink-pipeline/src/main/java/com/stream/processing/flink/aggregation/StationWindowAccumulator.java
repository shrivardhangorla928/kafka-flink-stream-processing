package com.stream.processing.flink.aggregation;

import com.stream.processing.common.MeasurementUnit;
import com.stream.processing.common.SensorType;

import java.io.Serializable;

/**
 * The running state of one station's window: the four reductions the pipeline needs, plus the
 * station metadata carried along so the emitted aggregate is self-describing.
 *
 * <p>This is what lives in Flink's managed state between records, so it is deliberately small and
 * fixed-size — a list of readings would grow without bound and turn a five-minute window into a
 * checkpointing problem.</p>
 *
 * <p>A mutable POJO with a no-argument constructor and matching getters and setters so Flink uses
 * its {@code PojoSerializer}; falling back to Kryo here would cost on every record.</p>
 */
public class StationWindowAccumulator implements Serializable {

    private static final long serialVersionUID = 1L;

    private String stationId;
    private String stationName;
    private String districtId;
    private SensorType sensorType;
    private MeasurementUnit unit;
    private double latitude;
    private double longitude;

    private long count;
    private double sum;
    private double min;
    private double max;

    public StationWindowAccumulator() {
        // Identity elements for min and max, so the first reading always replaces them.
        this.min = Double.POSITIVE_INFINITY;
        this.max = Double.NEGATIVE_INFINITY;
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

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
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

    public boolean isEmpty() {
        return count == 0L;
    }

    /** Mean over the window; zero for an empty accumulator rather than NaN, which JSON cannot carry. */
    public double average() {
        return count == 0L ? 0.0d : sum / count;
    }

    /** Minimum, normalised to zero while the accumulator is still empty. */
    public double minOrZero() {
        return count == 0L ? 0.0d : min;
    }

    /** Maximum, normalised to zero while the accumulator is still empty. */
    public double maxOrZero() {
        return count == 0L ? 0.0d : max;
    }

    /** A detached copy, so a result handed to the window function cannot alias live state. */
    public StationWindowAccumulator copy() {
        StationWindowAccumulator copy = new StationWindowAccumulator();
        copy.stationId = stationId;
        copy.stationName = stationName;
        copy.districtId = districtId;
        copy.sensorType = sensorType;
        copy.unit = unit;
        copy.latitude = latitude;
        copy.longitude = longitude;
        copy.count = count;
        copy.sum = sum;
        copy.min = min;
        copy.max = max;
        return copy;
    }

    @Override
    public String toString() {
        return "StationWindowAccumulator{stationId=" + stationId
                + ", sensorType=" + sensorType
                + ", count=" + count
                + ", sum=" + sum
                + ", min=" + minOrZero()
                + ", max=" + maxOrZero()
                + '}';
    }
}
