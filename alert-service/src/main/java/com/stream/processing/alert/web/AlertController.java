package com.stream.processing.alert.web;

import com.stream.processing.alert.repository.AlertFilter;
import com.stream.processing.alert.service.AlertQueryService;
import com.stream.processing.alert.web.dto.AcknowledgeRequest;
import com.stream.processing.alert.web.dto.AlertDto;
import com.stream.processing.alert.web.dto.AlertSummaryDto;
import com.stream.processing.alert.web.dto.PageResponse;
import com.stream.processing.alert.web.dto.StationWindowDto;
import com.stream.processing.common.SensorType;
import com.stream.processing.common.Severity;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Read API over the alert store: what the operations dashboard, the Grafana panels and the
 * evaluation scripts all query.
 *
 * <p>Every response is a DTO. Entities are never returned, so the schema can be migrated without
 * silently changing the public contract.</p>
 *
 * <p>Every binding annotation names its parameter explicitly. This build does not inherit
 * {@code spring-boot-starter-parent}, so the compiler's {@code -parameters} flag is not switched
 * on and Spring cannot recover argument names by reflection - without the names the whole API
 * fails at request time rather than at build time. Spelling them out also pins the query-string
 * contract to something a rename cannot silently break.</p>
 */
@RestController
@RequestMapping("/api/v1/alerts")
@Validated
public class AlertController {

    /** Range applied to {@code /summary} when the caller does not supply one. */
    private static final Duration DEFAULT_SUMMARY_WINDOW = Duration.ofHours(24);

    /** Rows returned by the station timeline when the caller does not supply a limit. */
    private static final int DEFAULT_TIMELINE_LIMIT = 100;

    private final AlertQueryService queryService;

    public AlertController(AlertQueryService queryService) {
        this.queryService = queryService;
    }

    /**
     * Paginated, filtered alert feed, newest first.
     *
     * <p>{@code severity} is a <em>minimum</em>, not an exact match: {@code severity=WARNING}
     * returns WARNING, SEVERE and EXTREME. That is what an operator means when they filter a
     * console for warnings, and it is why the row carries a denormalised severity rank.</p>
     *
     * <p>The requested page size is clamped server-side; a caller cannot widen it past
     * {@code stream.alert.api.max-page-size} however large a {@code size} parameter they send.</p>
     */
    @GetMapping
    public PageResponse<AlertDto> list(
            @RequestParam(name = "districtId", required = false) String districtId,
            @RequestParam(name = "stationId", required = false) String stationId,
            @RequestParam(name = "sensorType", required = false) SensorType sensorType,
            @RequestParam(name = "severity", required = false) Severity severity,
            @RequestParam(name = "from", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(name = "to", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(name = "acknowledged", required = false) Boolean acknowledged,
            @PageableDefault(size = 50) Pageable pageable) {

        AlertFilter filter =
                new AlertFilter(districtId, stationId, sensorType, severity, from, to, acknowledged);
        return queryService.search(filter, pageable);
    }

    /**
     * Aggregated counts for the dashboard tiles and the choropleth map.
     *
     * <p>Defaults to the last 24 hours rather than the whole table, so a panel that forgets its
     * range parameter degrades into a cheap query instead of a full scan.</p>
     */
    @GetMapping("/summary")
    public AlertSummaryDto summary(
            @RequestParam(name = "from", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(name = "to", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to) {

        Instant end = to != null ? to : Instant.now();
        Instant start = from != null ? from : end.minus(DEFAULT_SUMMARY_WINDOW);
        if (!start.isBefore(end)) {
            throw new InvalidRangeException(start, end);
        }
        return queryService.summary(start, end);
    }

    /** The recent windowed aggregates for one station, newest first. */
    @GetMapping("/stations/{stationId}/timeline")
    public List<StationWindowDto> timeline(
            @PathVariable(name = "stationId") @NotBlank String stationId,
            @RequestParam(name = "sensorType", required = false) SensorType sensorType,
            @RequestParam(name = "limit", defaultValue = "" + DEFAULT_TIMELINE_LIMIT) @Min(1) int limit) {

        return queryService.timeline(stationId, sensorType, limit);
    }

    /** A single alert, or 404. */
    @GetMapping("/{alertId}")
    public AlertDto get(@PathVariable(name = "alertId") @NotBlank String alertId) {
        return queryService.findById(alertId);
    }

    /**
     * Marks an alert as handled.
     *
     * <p>Idempotent: acknowledging an already-acknowledged alert returns 200 with the original
     * acknowledgement intact rather than an error, because the dashboard retries this call and a
     * retry must not look like a failure or overwrite the audit trail.</p>
     */
    @PostMapping("/{alertId}/acknowledge")
    public ResponseEntity<AlertDto> acknowledge(@PathVariable(name = "alertId") @NotBlank String alertId,
                                                @Valid @RequestBody AcknowledgeRequest request) {
        return ResponseEntity.ok(
                queryService.acknowledge(alertId, request.acknowledgedBy(), Instant.now()));
    }
}
