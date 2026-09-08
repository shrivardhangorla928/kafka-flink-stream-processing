package com.vassar.aware.alert.service;

import com.vassar.aware.alert.config.AlertServiceProperties;
import com.vassar.aware.alert.domain.AlertEntity;
import com.vassar.aware.alert.mapper.AlertMapper;
import com.vassar.aware.alert.mapper.StationWindowAggregateMapper;
import com.vassar.aware.alert.repository.AlertFilter;
import com.vassar.aware.alert.repository.AlertRepository;
import com.vassar.aware.alert.repository.AlertSpecifications;
import com.vassar.aware.alert.repository.StationWindowAggregateRepository;
import com.vassar.aware.alert.web.AlertNotFoundException;
import com.vassar.aware.alert.web.dto.AlertDto;
import com.vassar.aware.alert.web.dto.AlertSummaryDto;
import com.vassar.aware.alert.web.dto.PageResponse;
import com.vassar.aware.alert.web.dto.StationWindowDto;
import com.vassar.aware.common.SensorType;
import com.vassar.aware.common.Severity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.EnumMap;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Read side of the alert store, plus the one write the API exposes (acknowledgement).
 *
 * <p>All page requests pass through {@link #bounded(Pageable)} first. An operator - or a
 * misconfigured Grafana panel - asking for the unfiltered list must not be able to make the
 * database materialise the whole table, so the size is clamped server-side and a deterministic
 * sort is imposed whether or not the caller supplied one.</p>
 */
@Service
public class AlertQueryService {

    /**
     * Default and only meaningful ordering for an alert feed: newest first. Applied when the
     * caller does not specify a sort, and used as the tie-breaker guarantee that keeps pagination
     * stable - an unordered page request over a table that is being written to continuously can
     * otherwise return the same row on two consecutive pages.
     */
    private static final Sort DEFAULT_SORT =
            Sort.by(Sort.Direction.DESC, "generatedAt").and(Sort.by(Sort.Direction.ASC, "alertId"));

    private final AlertRepository alertRepository;
    private final StationWindowAggregateRepository aggregateRepository;
    private final AlertMapper alertMapper;
    private final StationWindowAggregateMapper aggregateMapper;
    private final AlertServiceProperties properties;

    public AlertQueryService(AlertRepository alertRepository,
                             StationWindowAggregateRepository aggregateRepository,
                             AlertMapper alertMapper,
                             StationWindowAggregateMapper aggregateMapper,
                             AlertServiceProperties properties) {
        this.alertRepository = alertRepository;
        this.aggregateRepository = aggregateRepository;
        this.alertMapper = alertMapper;
        this.aggregateMapper = aggregateMapper;
        this.properties = properties;
    }

    @Transactional(readOnly = true)
    public PageResponse<AlertDto> search(AlertFilter filter, Pageable pageable) {
        Page<AlertEntity> page =
                alertRepository.findAll(AlertSpecifications.matching(filter), bounded(pageable));
        return PageResponse.of(page, alertMapper::toDto);
    }

    @Transactional(readOnly = true)
    public AlertDto findById(String alertId) {
        return alertRepository.findById(alertId)
                .map(alertMapper::toDto)
                .orElseThrow(() -> new AlertNotFoundException(alertId));
    }

    /**
     * Acknowledges an alert, or does nothing if it is already acknowledged.
     *
     * <p>Idempotent on purpose: the dashboard retries this POST, and a second call must return
     * 200 with the existing acknowledgement rather than a conflict. The first responder's name
     * and timestamp are preserved.</p>
     *
     * @throws AlertNotFoundException when the id is unknown
     */
    @Transactional
    public AlertDto acknowledge(String alertId, String acknowledgedBy, Instant at) {
        AlertEntity entity = alertRepository.findById(alertId)
                .orElseThrow(() -> new AlertNotFoundException(alertId));
        entity.acknowledge(acknowledgedBy, truncateToStorableResolution(at));
        return alertMapper.toDto(alertRepository.save(entity));
    }

    /**
     * Drops precision the database cannot keep.
     *
     * <p>{@code Instant.now()} carries nanoseconds on Linux, but {@code timestamptz} stores
     * microseconds. Without this, the response to the first acknowledgement would report a
     * timestamp one digit finer than the one that was actually stored, and the retry - which
     * reads the row back - would answer with a different value for the same field. Idempotent
     * has to mean byte-identical, not approximately equal.</p>
     */
    private static Instant truncateToStorableResolution(Instant at) {
        return at == null ? null : at.truncatedTo(ChronoUnit.MICROS);
    }

    /**
     * Counts alerts in {@code [from, to)} by severity band and by district.
     *
     * <p>Both breakdowns are grouped by the database rather than in the JVM, and the range is
     * always present - the controller substitutes a default window when the caller omits it -
     * because an unbounded {@code GROUP BY} over the whole table is exactly the query that takes
     * a dashboard refresh from milliseconds to minutes.</p>
     */
    @Transactional(readOnly = true)
    public AlertSummaryDto summary(Instant from, Instant to) {
        Map<Severity, Long> bySeverity = new EnumMap<>(Severity.class);
        for (Severity severity : Severity.values()) {
            // Seed every band so the response shape is constant even when a band has no alerts.
            bySeverity.put(severity, 0L);
        }
        alertRepository.countBySeverity(from, to)
                .forEach(row -> bySeverity.put(row.getSeverity(), row.getTotal()));

        List<AlertSummaryDto.DistrictCountDto> byDistrict = alertRepository.countByDistrict(from, to)
                .stream()
                .map(row -> new AlertSummaryDto.DistrictCountDto(row.getDistrictId(), row.getTotal()))
                .sorted(Comparator.comparingLong(AlertSummaryDto.DistrictCountDto::count).reversed())
                .toList();

        return new AlertSummaryDto(from, to, alertRepository.countInRange(from, to),
                bySeverity, byDistrict);
    }

    /**
     * The most recent windows a station produced, newest first.
     *
     * @param limit caller-supplied bound, clamped to {@code aware.alert.api.max-timeline-size}
     */
    @Transactional(readOnly = true)
    public List<StationWindowDto> timeline(String stationId, SensorType sensorType, int limit) {
        Pageable page = PageRequest.ofSize(Math.min(Math.max(limit, 1), properties.api().maxTimelineSize()));
        List<com.vassar.aware.alert.domain.StationWindowAggregateEntity> rows = sensorType == null
                ? aggregateRepository.findByKeyStationIdOrderByKeyWindowStartDesc(stationId, page)
                : aggregateRepository.findByKeyStationIdAndKeySensorTypeOrderByKeyWindowStartDesc(
                        stationId, sensorType, page);
        return rows.stream().map(aggregateMapper::toDto).toList();
    }

    /**
     * Applies the server-side page-size cap and the default ordering.
     *
     * <p>Visible for testing: the cap is a contract of the API, not an implementation detail.</p>
     */
    Pageable bounded(Pageable pageable) {
        if (pageable == null || pageable.isUnpaged()) {
            return PageRequest.of(0, properties.api().defaultPageSize(), DEFAULT_SORT);
        }
        int size = Math.min(pageable.getPageSize(), properties.api().maxPageSize());
        Sort sort = pageable.getSort().isSorted() ? pageable.getSort() : DEFAULT_SORT;
        return PageRequest.of(pageable.getPageNumber(), size, sort);
    }
}
