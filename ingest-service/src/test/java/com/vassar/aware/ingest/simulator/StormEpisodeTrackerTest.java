package com.vassar.aware.ingest.simulator;

import com.vassar.aware.common.SensorType;
import com.vassar.aware.ingest.station.Station;
import com.vassar.aware.ingest.support.StationFixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the two properties the demonstration depends on: episodes start often enough that alerts
 * appear, and they end, so the pipeline is not permanently in alarm.
 */
class StormEpisodeTrackerTest {

    private static final List<Station> CATALOGUE = StationFixtures.catalogue(30, 12, 8);

    private static SimulatorSettings settings(double fraction, int minCycles, int maxCycles) {
        return new SimulatorSettings(42L, fraction, minCycles, maxCycles, 0.15d,
                4.0d, 12.0d, 86.0d, 101.5d, 8.2d, 12.6d);
    }

    @Test
    @DisplayName("the first cycle primes at least one episode per sensor type")
    void primesEverySensorType() {
        StormEpisodeTracker tracker = new StormEpisodeTracker(settings(0.12d, 5, 10), new Random(42L));

        tracker.advance(CATALOGUE);

        for (SensorType type : SensorType.values()) {
            boolean anyStorming = CATALOGUE.stream()
                    .filter(station -> station.sensorType() == type)
                    .anyMatch(station -> tracker.isStorming(station.stationId()));
            assertThat(anyStorming)
                    .as("at least one %s station storms in the opening cycle", type)
                    .isTrue();
        }
    }

    @Test
    @DisplayName("a primed round covers roughly the configured fraction of stations")
    void primesRoughlyTheConfiguredFraction() {
        StormEpisodeTracker tracker = new StormEpisodeTracker(settings(0.12d, 5, 10), new Random(7L));

        tracker.advance(CATALOGUE);

        // 30 * 0.12 -> 4, 12 * 0.12 -> 1, 8 * 0.12 -> 1
        assertThat(tracker.activeEpisodes()).isEqualTo(6);
    }

    @Test
    @DisplayName("a forced episode lasts at least the configured minimum number of cycles")
    void episodeLastsAtLeastMinimumCycles() {
        // stormFraction 0 removes priming and spontaneous starts, isolating the forced episode
        StormEpisodeTracker tracker = new StormEpisodeTracker(settings(0.0d, 5, 10), new Random(42L));
        List<Station> stations = StationFixtures.catalogue(1, 0, 0);
        String stationId = stations.get(0).stationId();

        assertThat(tracker.forceEpisodes(stations, 1)).isEqualTo(1);

        int stormingCycles = 0;
        for (int cycle = 0; cycle < 20; cycle++) {
            tracker.advance(stations);
            if (tracker.isStorming(stationId)) {
                stormingCycles++;
            } else {
                break;
            }
        }
        assertThat(stormingCycles).isBetween(5, 10);
    }

    @Test
    @DisplayName("episodes always end, so the pipeline is not permanently in alarm")
    void episodesExpire() {
        StormEpisodeTracker tracker = new StormEpisodeTracker(settings(0.0d, 5, 10), new Random(3L));
        tracker.forceEpisodes(CATALOGUE, CATALOGUE.size());
        assertThat(tracker.activeEpisodes()).isEqualTo(CATALOGUE.size());

        for (int cycle = 0; cycle < 11; cycle++) {
            tracker.advance(CATALOGUE);
        }

        assertThat(tracker.activeEpisodes()).isZero();
    }

    @Test
    @DisplayName("the long-run storming fraction tracks the configured value")
    void steadyStateFractionMatchesConfiguration() {
        StormEpisodeTracker tracker = new StormEpisodeTracker(settings(0.12d, 5, 10), new Random(11L));

        int cycles = 3_000;
        long totalStorming = 0;
        for (int cycle = 0; cycle < cycles; cycle++) {
            tracker.advance(CATALOGUE);
            totalStorming += tracker.activeEpisodes();
        }

        double observedFraction = (double) totalStorming / (cycles * (double) CATALOGUE.size());
        assertThat(observedFraction).isCloseTo(0.12d, org.assertj.core.data.Offset.offset(0.04d));
    }

    @Test
    @DisplayName("a zero storm fraction produces no episodes at all")
    void zeroFractionNeverStorms() {
        StormEpisodeTracker tracker = new StormEpisodeTracker(settings(0.0d, 5, 10), new Random(42L));

        for (int cycle = 0; cycle < 200; cycle++) {
            tracker.advance(CATALOGUE);
        }

        assertThat(tracker.activeEpisodes()).isZero();
        assertThat(tracker.snapshot()).isEmpty();
    }

    @Test
    @DisplayName("forcing more episodes than there are idle stations starts only what is available")
    void forcingIsCappedByIdleStations() {
        StormEpisodeTracker tracker = new StormEpisodeTracker(settings(0.0d, 5, 10), new Random(42L));
        List<Station> stations = StationFixtures.catalogue(3, 0, 0);

        assertThat(tracker.forceEpisodes(stations, 2)).isEqualTo(2);
        assertThat(tracker.forceEpisodes(stations, 5)).isEqualTo(1);
        assertThat(tracker.forceEpisodes(stations, 5)).isZero();
        assertThat(tracker.forceEpisodes(stations, 0)).isZero();
        assertThat(tracker.forceEpisodes(stations, -3)).isZero();
        assertThat(tracker.activeEpisodes()).isEqualTo(3);
    }

    @Test
    @DisplayName("the snapshot exposes the remaining cycles of each episode")
    void snapshotReportsRemainingCycles() {
        StormEpisodeTracker tracker = new StormEpisodeTracker(settings(0.0d, 5, 10), new Random(42L));
        List<Station> stations = StationFixtures.catalogue(1, 0, 0);
        tracker.forceEpisodes(stations, 1);

        assertThat(tracker.snapshot())
                .hasSize(1)
                .allSatisfy((stationId, remaining) -> assertThat(remaining).isBetween(5, 10));
    }
}
