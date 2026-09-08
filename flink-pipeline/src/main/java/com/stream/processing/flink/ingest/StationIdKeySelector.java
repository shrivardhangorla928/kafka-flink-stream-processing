package com.stream.processing.flink.ingest;

import com.stream.processing.common.SensorReading;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Keys the stream by station id.
 *
 * <p>A named class rather than a lambda: Flink extracts the key type from the declared generic
 * parameters, and a lambda erases them, which fails the job graph at submission time with
 * "the generic type parameters of KeySelector are missing".</p>
 */
public final class StationIdKeySelector implements KeySelector<SensorReading, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String getKey(SensorReading value) {
        return value.getStationId();
    }
}
