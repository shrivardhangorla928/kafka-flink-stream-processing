package com.vassar.aware.ingest.simulator;

import com.vassar.aware.common.MeasurementUnit;
import com.vassar.aware.common.SensorReading;
import com.vassar.aware.common.SensorType;
import com.vassar.aware.ingest.station.Station;
import com.vassar.aware.ingest.support.StationFixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The simulator is the only source of data in the dissertation's pipeline, so these assertions
 * are really assertions about the whole demonstration: a normal station must not trip the
 * downstream thresholds, a storming one must trip them, and the same seed must reproduce the
 * same run for the evaluation chapter.
 */
class ReadingSimulatorTest {

    /** Downstream Flink thresholds, restated here so a drift between the two shows up as a failure. */
    private static final double RAINFALL_WARNING_MM = 15.0d;
    private static final double RESERVOIR_WARNING_PERCENT = 85.0d;
    private static final double RIVER_WARNING_METRES = 8.0d;

    /** 5-minute tumbling window at a 60 s scrape interval. */
    private static final int READINGS_PER_WINDOW = 5;

    private static final Instant NOW = Instant.parse("2026-08-11T06:00:00Z");
    private static final String SOURCE = "AWARE_SIMULATOR";

    private static ReadingSimulator simulator(long seed) {
        SimulatorSettings defaults = SimulatorSettings.defaults();
        return new ReadingSimulator(new SimulatorSettings(seed, defaults.stormFraction(),
                defaults.stormMinCycles(), defaults.stormMaxCycles(), defaults.jitterFraction(),
                defaults.stormRainfallMinMm(), defaults.stormRainfallMaxMm(),
                defaults.stormReservoirMinPercent(), defaults.stormReservoirMaxPercent(),
                defaults.stormRiverMinMetres(), defaults.stormRiverMaxMetres()), SOURCE);
    }

    @Test
    @DisplayName("a round produces exactly one fully populated, valid reading per station")
    void roundProducesOneValidReadingPerStation() {
        List<Station> stations = StationFixtures.catalogue(30, 12, 8);

        List<SensorReading> readings = simulator(42L).simulateRound(stations, NOW);

        assertThat(readings).hasSameSizeAs(stations);
        assertThat(readings).allSatisfy(reading -> {
            assertThat(reading.isValid()).isTrue();
            assertThat(reading.getReadingId()).isNotBlank();
            assertThat(reading.getEventTime()).isEqualTo(NOW);
            assertThat(reading.getSource()).isEqualTo(SOURCE);
            assertThat(reading.getUnit()).isEqualTo(reading.getSensorType().getUnit());
            assertThat(reading.getIngestedAt()).as("stamped by the publisher, not here").isNull();
        });
        assertThat(readings).extracting(SensorReading::getStationId)
                .containsExactlyElementsOf(stations.stream().map(Station::stationId).toList());
        assertThat(readings).extracting(SensorReading::getReadingId).doesNotHaveDuplicates();
    }

    @Test
    @DisplayName("the same seed reproduces the same run, ids included")
    void sameSeedProducesIdenticalRuns() {
        List<Station> stations = StationFixtures.catalogue(10, 4, 2);
        ReadingSimulator first = simulator(42L);
        ReadingSimulator second = simulator(42L);

        for (int round = 0; round < 8; round++) {
            assertThat(first.simulateRound(stations, NOW))
                    .as("round %d", round)
                    .isEqualTo(second.simulateRound(stations, NOW));
        }
        assertThat(first.activeStormCount()).isEqualTo(second.activeStormCount());
    }

    @Test
    @DisplayName("a different seed produces a different run")
    void differentSeedProducesDifferentRuns() {
        List<Station> stations = StationFixtures.catalogue(10, 4, 2);

        List<SensorReading> first = simulator(42L).simulateRound(stations, NOW);
        List<SensorReading> second = simulator(4242L).simulateRound(stations, NOW);

        assertThat(first).isNotEqualTo(second);
    }

    @Test
    @DisplayName("a storming rainfall station fills a window into the alerting bands")
    void stormingRainfallBreachesTheWarningBand() {
        Station station = StationFixtures.rainfall("STN-0001", 0.8d);
        ReadingSimulator simulator = simulator(42L);

        double windowSum = 0.0d;
        for (int i = 0; i < READINGS_PER_WINDOW; i++) {
            double value = simulator.simulate(station, NOW, true).getValue();
            assertThat(value).isBetween(4.0d, 12.0d);
            windowSum += value;
        }

        assertThat(windowSum).isGreaterThanOrEqualTo(RAINFALL_WARNING_MM);
    }

    @Test
    @DisplayName("a normal rainfall station stays clear of the warning band")
    void normalRainfallStaysBelowTheWarningBand() {
        Station station = StationFixtures.rainfall("STN-0001", 1.6d);
        ReadingSimulator simulator = simulator(42L);

        for (int window = 0; window < 200; window++) {
            double windowSum = 0.0d;
            for (int i = 0; i < READINGS_PER_WINDOW; i++) {
                windowSum += simulator.simulate(station, NOW, false).getValue();
            }
            assertThat(windowSum).isLessThan(RAINFALL_WARNING_MM);
        }
    }

    @Test
    @DisplayName("storming level stations enter their own alerting bands")
    void stormingLevelStationsEnterTheirBands() {
        ReadingSimulator simulator = simulator(42L);
        Station reservoir = StationFixtures.reservoir("STN-0031", 60.0d);
        Station river = StationFixtures.river("STN-0043", 3.0d);

        for (int i = 0; i < 50; i++) {
            assertThat(simulator.simulate(reservoir, NOW, true).getValue())
                    .isGreaterThanOrEqualTo(RESERVOIR_WARNING_PERCENT);
            assertThat(simulator.simulate(river, NOW, true).getValue())
                    .isGreaterThanOrEqualTo(RIVER_WARNING_METRES);
        }
    }

    @Test
    @DisplayName("normal level stations stay clear of their alerting bands")
    void normalLevelStationsStayBelowTheirBands() {
        ReadingSimulator simulator = simulator(42L);
        Station reservoir = StationFixtures.reservoir("STN-0031", 60.0d);
        Station river = StationFixtures.river("STN-0043", 3.0d);

        for (int i = 0; i < 200; i++) {
            assertThat(simulator.simulate(reservoir, NOW, false).getValue())
                    .isLessThan(RESERVOIR_WARNING_PERCENT);
            assertThat(simulator.simulate(river, NOW, false).getValue())
                    .isLessThan(RIVER_WARNING_METRES);
        }
    }

    @Test
    @DisplayName("a storming reservoir never reports below its own baseline")
    void stormingReservoirNeverDipsBelowBaseline() {
        ReadingSimulator simulator = simulator(9L);
        Station highReservoir = StationFixtures.reservoir("STN-0031", 99.0d);

        for (int i = 0; i < 100; i++) {
            assertThat(simulator.simulate(highReservoir, NOW, true).getValue())
                    .isGreaterThanOrEqualTo(99.0d);
        }
    }

    @Test
    @DisplayName("no reading is ever negative, NaN or infinite")
    void valuesAreAlwaysFiniteAndNonNegative() {
        List<Station> stations = StationFixtures.catalogue(30, 12, 8);
        ReadingSimulator simulator = simulator(42L);

        for (int round = 0; round < 300; round++) {
            for (SensorReading reading : simulator.simulateRound(stations, NOW)) {
                double value = reading.getValue();
                assertThat(Double.isNaN(value)).isFalse();
                assertThat(Double.isInfinite(value)).isFalse();
                assertThat(value).isGreaterThanOrEqualTo(0.0d);
            }
        }
    }

    @Test
    @DisplayName("a run over the default catalogue produces alert-worthy rainfall windows")
    void defaultRunProducesAlertWorthyWindows() {
        List<Station> stations = StationFixtures.catalogue(30, 12, 8);
        ReadingSimulator simulator = simulator(42L);

        double[] windowSums = new double[stations.size()];
        for (int i = 0; i < READINGS_PER_WINDOW; i++) {
            List<SensorReading> readings = simulator.simulateRound(stations, NOW);
            for (int s = 0; s < readings.size(); s++) {
                if (readings.get(s).getSensorType() == SensorType.RAINFALL) {
                    windowSums[s] += readings.get(s).getValue();
                }
            }
        }

        long breaching = 0;
        for (double sum : windowSums) {
            if (sum >= RAINFALL_WARNING_MM) {
                breaching++;
            }
        }
        assertThat(breaching)
                .as("the very first window must already contain alerts, or a demo shows nothing")
                .isGreaterThanOrEqualTo(1L);
    }

    @Test
    @DisplayName("forcing storms puts the requested number of stations into an episode")
    void forcingStormsRaisesTheActiveCount() {
        List<Station> stations = StationFixtures.catalogue(30, 12, 8);
        ReadingSimulator simulator = simulator(42L);

        assertThat(simulator.forceStorms(stations, 5)).isEqualTo(5);
        assertThat(simulator.activeStormCount()).isEqualTo(5);
        assertThat(stations.stream().filter(s -> simulator.isStorming(s.stationId())).count()).isEqualTo(5L);
    }

    @Test
    @DisplayName("a station in a forced episode reports storm values in the very next round")
    void forcedEpisodeAffectsTheNextRound() {
        List<Station> stations = StationFixtures.catalogue(1, 0, 0);
        ReadingSimulator simulator = simulator(42L);
        simulator.forceStorms(stations, 1);

        SensorReading reading = simulator.simulateRound(stations, NOW).get(0);

        assertThat(reading.getValue()).isBetween(4.0d, 12.0d);
    }

    @Test
    @DisplayName("reservoir and river readings carry the units the pipeline expects")
    void readingsCarryTheSensorTypeUnit() {
        List<Station> stations = StationFixtures.catalogue(1, 1, 1);

        List<SensorReading> readings = simulator(42L).simulateRound(stations, NOW);

        assertThat(readings).extracting(SensorReading::getUnit)
                .containsExactly(MeasurementUnit.MM, MeasurementUnit.PERCENT, MeasurementUnit.METRE);
    }

    @Test
    @DisplayName("settings that could never breach a threshold are rejected at construction")
    void invalidSettingsAreRejected() {
        assertThatThrownBy(() -> new SimulatorSettings(1L, 0.1d, 0, 5, 0.1d,
                4, 12, 86, 101, 8, 12))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stormMinCycles");

        assertThatThrownBy(() -> new SimulatorSettings(1L, 0.1d, 6, 5, 0.1d,
                4, 12, 86, 101, 8, 12))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stormMaxCycles");

        assertThatThrownBy(() -> new SimulatorSettings(1L, 1.5d, 1, 5, 0.1d,
                4, 12, 86, 101, 8, 12))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stormFraction");

        assertThatThrownBy(() -> new SimulatorSettings(1L, 0.1d, 1, 5, -0.1d,
                4, 12, 86, 101, 8, 12))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jitterFraction");
    }

    @Test
    @DisplayName("mean storm length is the midpoint of the configured range")
    void meanStormCyclesIsTheMidpoint() {
        assertThat(SimulatorSettings.defaults().meanStormCycles()).isEqualTo(7.5d);
    }
}
