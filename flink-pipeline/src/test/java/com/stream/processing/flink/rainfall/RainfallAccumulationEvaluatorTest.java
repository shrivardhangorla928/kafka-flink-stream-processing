package com.stream.processing.flink.rainfall;

import com.stream.processing.common.Alert;
import com.stream.processing.common.MeasurementUnit;
import com.stream.processing.common.SensorReading;
import com.stream.processing.common.SensorType;
import com.stream.processing.common.Severity;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Boundary-by-boundary verification of the rolling-window rainfall accumulation policy.
 *
 * <p>Most cases here do not hardcode {@code RainfallAlertPolicy}'s band thresholds (only the one
 * figure the spec pins down -- 24h "extremely heavy" starting at 204.5 mm -- is a literal). Every
 * other scenario searches the real policy at test time for a sum that trips a given window into a
 * given severity, and places readings so that only the windows under test are affected, exploiting
 * that the six windows nest inside one another around the same {@code latestEventTime}. That keeps
 * the tests correct against whatever concrete band boundaries the policy ships with, while still
 * pinning down the accumulation and tie-break behaviour this class is responsible for.</p>
 */
class RainfallAccumulationEvaluatorTest {

    private static final Instant NOW = Instant.parse("2026-08-11T05:05:01Z");
    private static final Instant LATEST = Instant.parse("2026-08-11T05:00:00Z");
    private static final String STATION_ID = "STN-77";
    private static final String STATION_NAME = "Vizag";
    private static final String DISTRICT_ID = "D-VZG";
    private static final double LATITUDE = 17.68d;
    private static final double LONGITUDE = 83.21d;

    private final RainfallAccumulationEvaluator evaluator =
            new RainfallAccumulationEvaluator(Clock.fixed(NOW, ZoneOffset.UTC));

    private Optional<Alert> evaluate(List<SensorReading> readings) {
        return evaluator.evaluate(STATION_ID, STATION_NAME, DISTRICT_ID, LATITUDE, LONGITUDE,
                readings, LATEST);
    }

    private static SensorReading reading(double value, double hoursBeforeLatest) {
        SensorReading r = new SensorReading();
        r.setReadingId("R-" + hoursBeforeLatest);
        r.setStationId(STATION_ID);
        r.setStationName(STATION_NAME);
        r.setDistrictId(DISTRICT_ID);
        r.setSensorType(SensorType.RAINFALL);
        r.setValue(value);
        r.setUnit(MeasurementUnit.MM);
        r.setLatitude(LATITUDE);
        r.setLongitude(LONGITUDE);
        r.setEventTime(LATEST.minus(Math.round(hoursBeforeLatest * 3600), ChronoUnit.SECONDS));
        return r;
    }

    /** Smallest sum (searched upward in 0.5 mm steps) that trips ANY band for the given window. */
    private static double smallestTriggeringSum(int windowHours) {
        for (double mm = 0.5d; mm < 2000d; mm += 0.5d) {
            if (RainfallAlertPolicy.severityFor(windowHours, mm).isPresent()) {
                return mm;
            }
        }
        throw new IllegalStateException("no band found for window " + windowHours + "h under 2000 mm");
    }

    /** Smallest sum, searched upward from {@code startFrom}, that lands the window in {@code target}. */
    private static double sumForTargetSeverity(int windowHours, Severity target, double startFrom) {
        for (double mm = startFrom; mm < 2000d; mm += 0.5d) {
            Optional<Severity> severity = RainfallAlertPolicy.severityFor(windowHours, mm);
            if (severity.isPresent() && severity.get() == target) {
                return mm;
            }
        }
        throw new IllegalStateException(
                "no sum >= " + startFrom + " reaches " + target + " for window " + windowHours + "h");
    }

    @Test
    void staysQuietWhenNothingCrossesAnyBand() {
        List<SensorReading> readings = new ArrayList<>();
        for (int hour = 0; hour < 24; hour++) {
            readings.add(reading(0.1d, hour));
        }

        assertThat(evaluate(readings)).isEmpty();
    }

    @Test
    void reportsTheTwentyFourHourWindowWhenOnlyTheSlowSoakCrossesExtreme() {
        // 204.6 mm spread evenly across 24 hourly readings never bursts any single shorter window,
        // but the 24h total clears IMD's "extremely heavy rainfall" floor of 204.5 mm.
        List<SensorReading> readings = new ArrayList<>();
        double perHour = 204.6d / 24d;
        for (int hour = 0; hour < 24; hour++) {
            readings.add(reading(perHour, hour));
        }

        Alert alert = evaluate(readings).orElseThrow();

        assertThat(alert.getMetric()).isEqualTo("RAINFALL_ACCUMULATION_24H");
        assertThat(alert.getSeverity()).isEqualTo(Severity.EXTREME);
        assertThat(alert.getThresholdValue()).isEqualTo(204.5d);
        assertThat(alert.getObservedValue()).isEqualTo(204.6d, org.assertj.core.data.Offset.offset(0.01d));
        assertThat(alert.getWindowStart()).isEqualTo(LATEST.minus(24, ChronoUnit.HOURS));
        assertThat(alert.getWindowEnd()).isEqualTo(LATEST);
        assertThat(alert.getStationId()).isEqualTo(STATION_ID);
        assertThat(alert.getStationName()).isEqualTo(STATION_NAME);
        assertThat(alert.getDistrictId()).isEqualTo(DISTRICT_ID);
        assertThat(alert.getSensorType()).isEqualTo(SensorType.RAINFALL);
        assertThat(alert.getUnit()).isEqualTo(MeasurementUnit.MM);
        assertThat(alert.getGeneratedAt()).isEqualTo(NOW);
        assertThat(alert.getMessage())
                .startsWith("EXTREME severity:")
                .contains("Vizag (STN-77)")
                .contains("last 24 hours")
                .contains("204.5" /* the crossed floor, printed with 2 decimals as 204.50 */);
    }

    @Test
    void picksTwentyFourHoursOverAShorterWindowThatAlsoTriggers() {
        // Window=6 is the shortest window this policy ever alerts on at all (1,2,3 are configured
        // "ignore" at every band). A burst inside the last 6h trips window=6 on its own, but the
        // 24h total is driven past the EXTREME floor -- the highest rank any window can reach -- so
        // the 24h window must be reported regardless of what the 6h burst graded as.
        double burst = smallestTriggeringSum(6);
        assertThat(RainfallAlertPolicy.severityFor(6, burst)).isPresent();

        List<SensorReading> readings = new ArrayList<>();
        readings.add(reading(burst, 3d)); // inside the last 6h window
        double remaining = 204.6d - burst;
        int slices = 17;
        for (int i = 0; i < slices; i++) {
            // offsets 7h..23h: outside the 6h window, inside the 24h window
            readings.add(reading(remaining / slices, 7d + i));
        }

        Alert alert = evaluate(readings).orElseThrow();

        assertThat(alert.getMetric()).isEqualTo("RAINFALL_ACCUMULATION_24H");
        assertThat(alert.getSeverity()).isEqualTo(Severity.EXTREME);
    }

    @Test
    void breaksATieBetweenTwoWindowsInFavourOfTheLargerOne() {
        // A single reading placed in the (6h, 12h] gap is seen only by windows 12 and 24 (windows
        // 1,2,3,6 exclude it entirely). Grow it to the smallest sum that reaches EXTREME for
        // window=24; window=12's own EXTREME floor is lower than window=24's, so that same total
        // also reads as EXTREME through the 12h window -- a genuine tie in severity between two
        // different windows. Per the ">=" tie-break rule documented on the evaluator, the larger
        // (24h) window must be the one reported.
        double sum = sumForTargetSeverity(24, Severity.EXTREME, 0.5d);
        Severity target = RainfallAlertPolicy.severityFor(24, sum).orElseThrow();
        assertThat(RainfallAlertPolicy.severityFor(12, sum)).contains(target);

        List<SensorReading> readings = new ArrayList<>();
        readings.add(reading(sum, 9d)); // in (6h,12h]: seen by windows 12 and 24 only

        Alert alert = evaluate(readings).orElseThrow();

        assertThat(alert.getSeverity()).isEqualTo(target);
        assertThat(alert.getMetric()).isEqualTo("RAINFALL_ACCUMULATION_24H");
    }

    @Test
    void ignoresReadingsOlderThanTwentyFourHoursEvenIfPresentInTheBufferedList() {
        List<SensorReading> readings = new ArrayList<>();
        readings.add(reading(9999d, 30d)); // 30h old: outside every window, huge on purpose
        for (int hour = 0; hour < 24; hour++) {
            readings.add(reading(0.1d, hour)); // never enough to cross any band
        }

        assertThat(evaluate(readings)).isEmpty();
    }

    @Test
    void derivesAStableAlertIdFromTheFactsThatProducedIt() {
        List<SensorReading> readings = new ArrayList<>();
        double perHour = 204.6d / 24d;
        for (int hour = 0; hour < 24; hour++) {
            readings.add(reading(perHour, hour));
        }

        Alert first = evaluate(readings).orElseThrow();
        Alert second = evaluate(readings).orElseThrow();

        assertThat(second.getAlertId()).isEqualTo(first.getAlertId());
    }

    @Test
    void fallsBackToTheStationIdWhenTheNameIsMissing() {
        List<SensorReading> readings = new ArrayList<>();
        double perHour = 204.6d / 24d;
        for (int hour = 0; hour < 24; hour++) {
            readings.add(reading(perHour, hour));
        }

        Optional<Alert> alert = evaluator.evaluate(
                STATION_ID, null, DISTRICT_ID, LATITUDE, LONGITUDE, readings, LATEST);

        assertThat(alert).isPresent();
        assertThat(alert.get().getMessage()).contains(STATION_ID + " recorded");
    }

    @Test
    void returnsEmptyWithoutThrowingForNullOrEmptyInputs() {
        List<SensorReading> some = List.of(reading(1.0d, 0));

        assertThatCode(() -> {
            assertThat(evaluator.evaluate(null, STATION_NAME, DISTRICT_ID, LATITUDE, LONGITUDE, some, LATEST))
                    .isEmpty();
            assertThat(evaluator.evaluate(STATION_ID, STATION_NAME, DISTRICT_ID, LATITUDE, LONGITUDE, some, null))
                    .isEmpty();
            assertThat(evaluator.evaluate(STATION_ID, STATION_NAME, DISTRICT_ID, LATITUDE, LONGITUDE, null, LATEST))
                    .isEmpty();
            assertThat(evaluator.evaluate(
                    STATION_ID, STATION_NAME, DISTRICT_ID, LATITUDE, LONGITUDE, List.of(), LATEST))
                    .isEmpty();
        }).doesNotThrowAnyException();
    }
}
