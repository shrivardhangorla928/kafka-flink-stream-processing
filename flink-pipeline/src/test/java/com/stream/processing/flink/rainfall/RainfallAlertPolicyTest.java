package com.stream.processing.flink.rainfall;

import com.stream.processing.common.Severity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Boundary-by-boundary verification of the ported rainfall band/alert-level tables. */
class RainfallAlertPolicyTest {

    @ParameterizedTest(name = "window=1h, {0}mm -> {1}")
    @CsvSource({
            "0.0,   no_rainfall",
            "0.099, no_rainfall",
            "0.1,   very_light_rainfall",
            "1.0,   very_light_rainfall",
            "1.1,   light_rainfall",
            "4.0,   light_rainfall",
            "4.1,   moderate_rainfall",
            "16.0,  moderate_rainfall",
            "16.1,  heavy_rainfall",
            "30.0,  heavy_rainfall",
            "30.1,  very_heavy_rainfall",
            "49.999,very_heavy_rainfall",
            "50.0,  extremely_heavy_rainfall",
            "500.0, extremely_heavy_rainfall"
    })
    void bandLabelFor_window1(double accumulatedMm, String expectedBand) {
        assertThat(RainfallAlertPolicy.bandLabelFor(1, accumulatedMm)).isEqualTo(expectedBand);
    }

    @ParameterizedTest(name = "window=24h, {0}mm -> {1}")
    @CsvSource({
            "0.0,    no_rainfall",
            "0.399,  no_rainfall",
            "0.4,    very_light_rainfall",
            "2.5,    very_light_rainfall",
            "2.6,    light_rainfall",
            "15.6,   light_rainfall",
            "15.7,   moderate_rainfall",
            "64.5,   moderate_rainfall",
            "64.6,   heavy_rainfall",
            "115.6,  heavy_rainfall",
            "115.7,  very_heavy_rainfall",
            "204.499,very_heavy_rainfall",
            "204.5,  extremely_heavy_rainfall",
            "1000.0, extremely_heavy_rainfall"
    })
    void bandLabelFor_window24(double accumulatedMm, String expectedBand) {
        assertThat(RainfallAlertPolicy.bandLabelFor(24, accumulatedMm)).isEqualTo(expectedBand);
    }

    @Test
    void severityFor_window1_isAlwaysIgnoredRegardlessOfValue() {
        // window=1h has every band (including heavy/very_heavy/extremely_heavy) configured as
        // "ignore" -- a passing shower, however intense, never alerts on its own.
        assertThat(RainfallAlertPolicy.severityFor(1, 0.0)).isEmpty();
        assertThat(RainfallAlertPolicy.severityFor(1, 20.0)).isEmpty();
        assertThat(RainfallAlertPolicy.severityFor(1, 500.0)).isEmpty();
    }

    @Test
    void severityFor_window24_extremelyHeavy_isExtreme() {
        // raw "alert" -> Severity.EXTREME
        assertThat(RainfallAlertPolicy.severityFor(24, 250.0)).isEqualTo(Optional.of(Severity.EXTREME));
    }

    @Test
    void severityFor_window24_veryHeavy_isSevere_notSeverityWarning() {
        // The source system's raw level here is its own "warning", which maps to Severity.SEVERE
        // -- NOT Severity.WARNING. The two "warning" words are unrelated vocabularies; see the
        // naming-collision javadoc on RainfallAlertPolicy.
        assertThat(RainfallAlertPolicy.severityFor(24, 150.0)).isEqualTo(Optional.of(Severity.SEVERE));
    }

    @Test
    void severityFor_window6_extremelyHeavy_isWarning() {
        // raw "caution" -> Severity.WARNING
        assertThat(RainfallAlertPolicy.severityFor(6, 120.0)).isEqualTo(Optional.of(Severity.WARNING));
    }

    @Test
    void bandLowerBoundFor_returnsConfiguredBoundaries() {
        assertThat(RainfallAlertPolicy.bandLowerBoundFor(1, "heavy_rainfall")).isEqualTo(16.1);
        assertThat(RainfallAlertPolicy.bandLowerBoundFor(24, "extremely_heavy_rainfall")).isEqualTo(204.5);
    }

    @Test
    void bandLowerBoundFor_unknownLabel_throws() {
        assertThatThrownBy(() -> RainfallAlertPolicy.bandLowerBoundFor(1, "not_a_band"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void unknownWindow_throwsFromBandLabelForAndSeverityFor() {
        assertThatThrownBy(() -> RainfallAlertPolicy.bandLabelFor(5, 10.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RainfallAlertPolicy.severityFor(5, 10.0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void humanize_formatsLabel() {
        assertThat(RainfallAlertPolicy.humanize("very_heavy_rainfall")).isEqualTo("Very Heavy Rainfall");
        assertThat(RainfallAlertPolicy.humanize("no_rainfall")).isEqualTo("No Rainfall");
    }
}
