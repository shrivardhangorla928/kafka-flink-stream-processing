package com.stream.processing.alert.web.dto;

import com.stream.processing.common.Severity;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Aggregated counts over a time range, shaped for the dashboard tiles and the Grafana panels
 * rather than for generic consumption.
 *
 * <p>{@code bySeverity} always carries an entry for every {@link Severity} band, zero included:
 * a chart that silently drops the empty bands changes shape between refreshes, which is exactly
 * what a monitoring panel must not do.</p>
 *
 * @param from       inclusive start of the range that was counted
 * @param to         exclusive end of the range that was counted
 * @param total      alerts in the range
 * @param bySeverity count per severity band
 * @param byDistrict count per district, highest first
 */
public record AlertSummaryDto(Instant from,
                              Instant to,
                              long total,
                              Map<Severity, Long> bySeverity,
                              List<DistrictCountDto> byDistrict) {

    /**
     * @param districtId the district, or {@code null} for alerts whose station has no district
     * @param count      alerts attributed to it
     */
    public record DistrictCountDto(String districtId, long count) {
    }
}
