package com.stream.processing.ingest.simulator;

import com.stream.processing.common.SensorReading;
import com.stream.processing.common.SensorType;
import com.stream.processing.ingest.station.Station;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;

/**
 * Produces the synthetic observations that stand in for the real scrapers.
 *
 * <p>The point of the simulation is not realism for its own sake but giving the downstream Flink
 * stage something to alert on: a station behaving normally must stay clear of its threshold band
 * so the pipeline's selectivity is visible, and a station in a storm episode must cross it
 * reliably so the end-to-end latency of an alert can be measured. The storm bands are therefore
 * expressed in the same units as the downstream thresholds and configured, not hard-coded.</p>
 *
 * <p>Every random draw comes from one seeded {@link Random} and stations are always visited in
 * catalogue order, so two runs with the same seed produce byte-identical rounds - including the
 * reading ids, which are drawn from the same stream rather than from
 * {@link UUID#randomUUID()}. The evaluation chapter depends on that reproducibility.</p>
 *
 * <p>No Spring or Kafka types appear here on purpose; the class is constructed by
 * {@link SimulatorConfig} and is otherwise a plain object under test.</p>
 */
public final class ReadingSimulator {

    private final SimulatorSettings settings;
    private final String source;
    private final Random random;
    private final StormEpisodeTracker storms;

    public ReadingSimulator(SimulatorSettings settings, String source) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.source = Objects.requireNonNull(source, "source");
        this.random = new Random(settings.seed());
        this.storms = new StormEpisodeTracker(settings, random);
    }

    /**
     * Advances the storm state by one cycle and produces exactly one reading per station.
     *
     * @param stations  the stations to poll, in a stable order
     * @param eventTime the observation time stamped onto every reading of the round
     */
    public synchronized List<SensorReading> simulateRound(List<Station> stations, Instant eventTime) {
        Objects.requireNonNull(stations, "stations");
        Objects.requireNonNull(eventTime, "eventTime");

        storms.advance(stations);

        List<SensorReading> readings = new ArrayList<>(stations.size());
        for (Station station : stations) {
            readings.add(simulate(station, eventTime, storms.isStorming(station.stationId())));
        }
        return readings;
    }

    /** Forces storm episodes onto stations that are not already storming; returns how many started. */
    public synchronized int forceStorms(List<Station> stations, int count) {
        return storms.forceEpisodes(stations, count);
    }

    public synchronized int activeStormCount() {
        return storms.activeEpisodes();
    }

    public synchronized boolean isStorming(String stationId) {
        return storms.isStorming(stationId);
    }

    /**
     * Builds a single reading. Exposed so a test can pin the storm flag instead of having to
     * drive the tracker into the state it wants.
     */
    public synchronized SensorReading simulate(Station station, Instant eventTime, boolean storming) {
        SensorReading reading = new SensorReading();
        reading.setReadingId(nextReadingId());
        reading.setStationId(station.stationId());
        reading.setStationName(station.stationName());
        reading.setDistrictId(station.districtId());
        reading.setSensorType(station.sensorType());
        reading.setUnit(station.sensorType().getUnit());
        reading.setValue(storming ? stormValue(station) : normalValue(station));
        reading.setLatitude(station.latitude());
        reading.setLongitude(station.longitude());
        reading.setEventTime(eventTime);
        reading.setSource(source);
        return reading;
    }

    /**
     * Jitters symmetrically around the baseline and clamps at zero: a negative rainfall reading
     * would be rejected by {@link SensorReading#isValid()} and land in the dead-letter topic,
     * which would be a bug in the simulator rather than a fault worth demonstrating.
     */
    private double normalValue(Station station) {
        double factor = 1.0d + (random.nextDouble() * 2.0d - 1.0d) * settings.jitterFraction();
        return round2(Math.max(0.0d, station.baseline() * factor));
    }

    /**
     * Draws inside the sensor type's storm band. Levels take the higher of the band draw and the
     * station's own baseline, because a reservoir that normally sits at 95% must not appear to
     * fall when the storm starts.
     */
    private double stormValue(Station station) {
        SensorType type = station.sensorType();
        double drawn = switch (type) {
            case RAINFALL -> uniform(settings.stormRainfallMinMm(), settings.stormRainfallMaxMm());
            case RESERVOIR_LEVEL -> Math.max(station.baseline(),
                    uniform(settings.stormReservoirMinPercent(), settings.stormReservoirMaxPercent()));
            case RIVER_LEVEL -> Math.max(station.baseline(),
                    uniform(settings.stormRiverMinMetres(), settings.stormRiverMaxMetres()));
        };
        return round2(Math.max(0.0d, drawn));
    }

    private double uniform(double min, double max) {
        return min + random.nextDouble() * (max - min);
    }

    private String nextReadingId() {
        return new UUID(random.nextLong(), random.nextLong()).toString();
    }

    private static double round2(double value) {
        return Math.round(value * 100.0d) / 100.0d;
    }
}
