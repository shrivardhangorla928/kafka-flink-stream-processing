package com.vassar.aware.ingest.support;

import com.vassar.aware.common.SensorType;
import com.vassar.aware.ingest.station.Station;

import java.util.ArrayList;
import java.util.List;

/**
 * Builders for small, hand-controlled station catalogues.
 *
 * <p>Tests that assert on simulator behaviour use these rather than the bundled catalogue so a
 * later edit to {@code stations.json} cannot silently change what an assertion means.</p>
 */
public final class StationFixtures {

    private StationFixtures() {
        // static holder
    }

    public static Station rainfall(String id, double baseline) {
        return new Station(id, id + " Rain Gauge", "AP-KRISHNA", SensorType.RAINFALL,
                16.5d, 80.6d, baseline, true);
    }

    public static Station reservoir(String id, double baselinePercent) {
        return new Station(id, id + " Reservoir", "AP-NANDYAL", SensorType.RESERVOIR_LEVEL,
                15.87d, 78.9d, baselinePercent, true);
    }

    public static Station river(String id, double baselineMetres) {
        return new Station(id, id + " Gauge", "AP-NTR", SensorType.RIVER_LEVEL,
                16.51d, 80.61d, baselineMetres, true);
    }

    public static Station disabled(String id) {
        return new Station(id, id + " Rain Gauge", "AP-GUNTUR", SensorType.RAINFALL,
                16.3d, 80.44d, 1.0d, false);
    }

    /** A catalogue with the requested number of stations of each type, ids {@code STN-0001..}. */
    public static List<Station> catalogue(int rainfall, int reservoir, int river) {
        List<Station> stations = new ArrayList<>(rainfall + reservoir + river);
        int seq = 1;
        for (int i = 0; i < rainfall; i++) {
            stations.add(rainfall(id(seq++), 0.5d + i * 0.05d));
        }
        for (int i = 0; i < reservoir; i++) {
            stations.add(reservoir(id(seq++), 60.0d + i));
        }
        for (int i = 0; i < river; i++) {
            stations.add(river(id(seq++), 3.0d + i * 0.25d));
        }
        return List.copyOf(stations);
    }

    private static String id(int sequence) {
        return String.format("STN-%04d", sequence);
    }
}
