package com.stream.processing.flink.rainfall;

import com.stream.processing.common.Severity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * IMD-style, duration-calibrated rainfall alerting policy, ported verbatim from a separate live
 * production system.
 *
 * <p>The same accumulated rainfall means something different depending on how long it took to
 * fall: 50mm in one hour is a passing cloudburst, but 50mm sustained over 24 hours barely
 * registers. This policy therefore classifies an accumulated value into an intensity
 * <em>band</em> (e.g. {@code "heavy_rainfall"}) separately for each of six rolling windows (1,
 * 2, 3, 6, 12, 24 hours), and only some (window, band) combinations are configured to raise an
 * alert at all &mdash; sensitivity increases with window length.</p>
 *
 * <p>Kept free of any Flink type on purpose, mirroring {@link com.stream.processing.flink.config.ThresholdRule}:
 * this is the part of the job a domain expert will want to argue about, so it has to be readable
 * and unit-testable without starting a cluster.</p>
 *
 * <h2>Naming collision warning</h2>
 * <p>The source system's raw alert-level strings are {@code ignore} / {@code caution} /
 * {@code warning} / {@code alert}. This is a <b>different vocabulary</b> from this project's
 * {@link Severity} enum ({@code INFO} / {@code WARNING} / {@code SEVERE} / {@code EXTREME}), and
 * the word "warning" appears in <b>both</b> with <b>different meaning</b>. The mapping in
 * {@link #rawLevelToSeverity(String)} is:</p>
 * <ul>
 *     <li>{@code "ignore"} &rarr; no alert ({@link Optional#empty()}, never {@code Severity.INFO})</li>
 *     <li>{@code "caution"} &rarr; {@link Severity#WARNING}</li>
 *     <li>the source system's {@code "warning"} &rarr; {@link Severity#SEVERE} (<b>not</b> {@code Severity.WARNING}!)</li>
 *     <li>{@code "alert"} &rarr; {@link Severity#EXTREME}</li>
 * </ul>
 */
public final class RainfallAlertPolicy implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The accumulation windows this policy defines, ascending. */
    public static final List<Integer> WINDOW_HOURS_ASCENDING = List.of(1, 2, 3, 6, 12, 24);

    private static final String NO_RAINFALL = "no_rainfall";
    private static final String VERY_LIGHT_RAINFALL = "very_light_rainfall";
    private static final String LIGHT_RAINFALL = "light_rainfall";
    private static final String MODERATE_RAINFALL = "moderate_rainfall";
    private static final String HEAVY_RAINFALL = "heavy_rainfall";
    private static final String VERY_HEAVY_RAINFALL = "very_heavy_rainfall";
    private static final String EXTREMELY_HEAVY_RAINFALL = "extremely_heavy_rainfall";

    /** windowHours -> the 7 bands for that window, in ascending order, contiguous over [0, +INF). */
    private static final Map<Integer, List<RainfallBand>> BANDS_BY_WINDOW;

    /** windowHours -> (bandLabel -> Severity), only the non-"ignore" entries; a missing lookup means ignore. */
    private static final Map<Integer, Map<String, Severity>> SEVERITY_BY_WINDOW;

    static {
        Map<Integer, List<RainfallBand>> bands = new LinkedHashMap<>();
        bands.put(1, bandsOf(0.0, 0.1, 1.1, 4.1, 16.1, 30.1, 50.0));
        bands.put(2, bandsOf(0.0, 0.1, 1.6, 6.1, 24.1, 45.0, 75.2));
        bands.put(3, bandsOf(0.0, 0.1, 1.8, 7.1, 28.1, 52.3, 87.9));
        bands.put(6, bandsOf(0.0, 0.2, 2.1, 9.0, 36.2, 66.8, 113.4));
        bands.put(12, bandsOf(0.0, 0.3, 2.5, 12.3, 49.9, 91.1, 157.1));
        bands.put(24, bandsOf(0.0, 0.4, 2.6, 15.7, 64.6, 115.7, 204.5));

        Map<Integer, List<RainfallBand>> defensiveBands = new LinkedHashMap<>();
        bands.forEach((window, list) -> defensiveBands.put(window, List.copyOf(list)));
        BANDS_BY_WINDOW = Collections.unmodifiableMap(defensiveBands);

        Map<Integer, Map<String, Severity>> levels = new LinkedHashMap<>();
        levels.put(1, levelsOf(
                HEAVY_RAINFALL, "ignore",
                VERY_HEAVY_RAINFALL, "ignore",
                EXTREMELY_HEAVY_RAINFALL, "ignore"));
        levels.put(2, levelsOf(
                HEAVY_RAINFALL, "ignore",
                VERY_HEAVY_RAINFALL, "ignore",
                EXTREMELY_HEAVY_RAINFALL, "ignore"));
        levels.put(3, levelsOf(
                HEAVY_RAINFALL, "ignore",
                VERY_HEAVY_RAINFALL, "ignore",
                EXTREMELY_HEAVY_RAINFALL, "ignore"));
        levels.put(6, levelsOf(
                HEAVY_RAINFALL, "ignore",
                VERY_HEAVY_RAINFALL, "ignore",
                EXTREMELY_HEAVY_RAINFALL, "caution"));
        levels.put(12, levelsOf(
                HEAVY_RAINFALL, "ignore",
                VERY_HEAVY_RAINFALL, "caution",
                EXTREMELY_HEAVY_RAINFALL, "alert"));
        levels.put(24, levelsOf(
                HEAVY_RAINFALL, "ignore",
                VERY_HEAVY_RAINFALL, "warning",
                EXTREMELY_HEAVY_RAINFALL, "alert"));

        Map<Integer, Map<String, Severity>> defensiveLevels = new LinkedHashMap<>();
        levels.forEach((window, map) -> defensiveLevels.put(window, Collections.unmodifiableMap(new LinkedHashMap<>(map))));
        SEVERITY_BY_WINDOW = Collections.unmodifiableMap(defensiveLevels);
    }

    private RainfallAlertPolicy() {
    }

    /**
     * Builds the 7 contiguous bands for a window from its 6 interior boundaries
     * ({@code min(no_rainfall), min(very_light), min(light), min(moderate), min(heavy),
     * min(very_heavy), min(extremely_heavy)}); the top band always ends at +INF.
     */
    private static List<RainfallBand> bandsOf(double noRainfallMin, double veryLightMin, double lightMin,
            double moderateMin, double heavyMin, double veryHeavyMin, double extremelyHeavyMin) {
        List<RainfallBand> list = new ArrayList<>(7);
        list.add(new RainfallBand(NO_RAINFALL, noRainfallMin, veryLightMin));
        list.add(new RainfallBand(VERY_LIGHT_RAINFALL, veryLightMin, lightMin));
        list.add(new RainfallBand(LIGHT_RAINFALL, lightMin, moderateMin));
        list.add(new RainfallBand(MODERATE_RAINFALL, moderateMin, heavyMin));
        list.add(new RainfallBand(HEAVY_RAINFALL, heavyMin, veryHeavyMin));
        list.add(new RainfallBand(VERY_HEAVY_RAINFALL, veryHeavyMin, extremelyHeavyMin));
        list.add(new RainfallBand(EXTREMELY_HEAVY_RAINFALL, extremelyHeavyMin, Double.POSITIVE_INFINITY));
        return list;
    }

    /** Convenience for building a small (label -> raw level string) map inline in the static block. */
    private static Map<String, String> rawLevelsOf(String... labelLevelPairs) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < labelLevelPairs.length; i += 2) {
            map.put(labelLevelPairs[i], labelLevelPairs[i + 1]);
        }
        return map;
    }

    /** Same as {@link #rawLevelsOf}, but converts each raw level straight to a {@link Severity}, dropping "ignore" entries. */
    private static Map<String, Severity> levelsOf(String... labelLevelPairs) {
        Map<String, Severity> map = new LinkedHashMap<>();
        rawLevelsOf(labelLevelPairs).forEach((label, rawLevel) -> {
            Optional<Severity> severity = rawLevelToSeverity(rawLevel);
            severity.ifPresent(value -> map.put(label, value));
        });
        return map;
    }

    /**
     * Maps a raw source-system alert-level string to this project's {@link Severity}. See the
     * class javadoc: this vocabulary is unrelated to, and partially overlaps in spelling with,
     * {@link Severity}'s own names.
     */
    private static Optional<Severity> rawLevelToSeverity(String rawLevel) {
        return switch (rawLevel) {
            case "ignore" -> Optional.empty();
            case "caution" -> Optional.of(Severity.WARNING);
            case "warning" -> Optional.of(Severity.SEVERE);
            case "alert" -> Optional.of(Severity.EXTREME);
            default -> throw new IllegalArgumentException("Unknown raw alert level: " + rawLevel);
        };
    }

    /**
     * The intensity band label for an accumulated value over the given window (hours).
     *
     * @throws IllegalArgumentException if {@code windowHours} isn't one of
     *         {@link #WINDOW_HOURS_ASCENDING}, or if the value matches no band (should never
     *         happen given the tables above, since they're contiguous 0..+INF, but validated
     *         anyway).
     */
    public static String bandLabelFor(int windowHours, double accumulatedMm) {
        for (RainfallBand band : bandsFor(windowHours)) {
            if (band.matches(accumulatedMm)) {
                return band.label();
            }
        }
        throw new IllegalArgumentException("No rainfall band matches value " + accumulatedMm
                + " for window=" + windowHours + "h");
    }

    /**
     * The lower bound of a named band for a window &mdash; used to quote "the threshold that was
     * crossed" in an alert message.
     *
     * @throws IllegalArgumentException for an unknown window/label.
     */
    public static double bandLowerBoundFor(int windowHours, String bandLabel) {
        for (RainfallBand band : bandsFor(windowHours)) {
            if (band.label().equals(bandLabel)) {
                return band.minInclusive();
            }
        }
        throw new IllegalArgumentException("Unknown rainfall band \"" + bandLabel
                + "\" for window=" + windowHours + "h");
    }

    /** Severity for an accumulated value over a window, or empty when it's ignore / not configured. */
    public static Optional<Severity> severityFor(int windowHours, double accumulatedMm) {
        String bandLabel = bandLabelFor(windowHours, accumulatedMm);
        Map<String, Severity> levels = SEVERITY_BY_WINDOW.get(windowHours);
        return Optional.ofNullable(levels.get(bandLabel));
    }

    /** Human-readable form of a band label, e.g. "very_heavy_rainfall" -&gt; "Very Heavy Rainfall". */
    public static String humanize(String bandLabel) {
        String[] words = bandLabel.split("_");
        StringBuilder result = new StringBuilder();
        for (String word : words) {
            if (word.isEmpty()) {
                continue;
            }
            if (result.length() > 0) {
                result.append(' ');
            }
            result.append(Character.toUpperCase(word.charAt(0))).append(word.substring(1));
        }
        return result.toString();
    }

    private static List<RainfallBand> bandsFor(int windowHours) {
        List<RainfallBand> bands = BANDS_BY_WINDOW.get(windowHours);
        if (bands == null) {
            throw new IllegalArgumentException("Unsupported rainfall window: " + windowHours
                    + "h (must be one of " + WINDOW_HOURS_ASCENDING + ")");
        }
        return bands;
    }
}
