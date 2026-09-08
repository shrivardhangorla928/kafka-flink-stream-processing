package com.stream.processing.alert.service;

import com.stream.processing.alert.domain.AlertEntity;
import com.stream.processing.alert.mapper.AlertMapper;
import com.stream.processing.alert.repository.AlertRepository;
import com.stream.processing.common.Alert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Idempotent write path for alerts.
 *
 * <p>Flink's Kafka sink is at-least-once, so the same deterministic {@code alertId} legitimately
 * arrives more than once - after a checkpoint restore, after a rebalance, after this service
 * itself fails between persisting and acknowledging. Storing a batch is therefore an upsert
 * keyed on {@code alertId}, never a blind insert, and a repeat is reported back as a duplicate
 * rather than counted as a new alert.</p>
 *
 * <p>Implemented as read-the-existing-ids then insert-or-update rather than a database-native
 * {@code ON CONFLICT}: two round trips instead of one, but it keeps the SQL portable to the
 * H2-backed tests and - more usefully - it is the only formulation that can tell the caller
 * which ids were new, which the metrics depend on.</p>
 */
@Service
public class AlertPersistenceService {

    private static final Logger log = LoggerFactory.getLogger(AlertPersistenceService.class);

    private final AlertRepository repository;
    private final AlertMapper mapper;

    public AlertPersistenceService(AlertRepository repository, AlertMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    /**
     * Stores a whole poll batch in one transaction.
     *
     * <p>One transaction per batch, not per record: the consumer acknowledges only after this
     * returns, so an all-or-nothing commit is what lets a failed batch be replayed safely.</p>
     *
     * @param alerts     the batch, in offset order
     * @param receivedAt the instant stamped on every new row in this batch
     * @return which ids were new and how many records were seen
     */
    @Transactional
    public PersistResult persist(List<Alert> alerts, Instant receivedAt) {
        if (alerts.isEmpty()) {
            return new PersistResult(Collections.emptySet(), 0);
        }

        // Collapse repeats inside the batch first. A replayed Flink window can put the same id
        // twice in one poll, and letting both through would mean two entity instances competing
        // for one row inside a single persistence context.
        Map<String, Alert> byId = new LinkedHashMap<>();
        for (Alert alert : alerts) {
            if (alert.getAlertId() == null || alert.getAlertId().isBlank()) {
                // Cannot be made idempotent without a key; dropping is safer than inventing one,
                // because an invented key would let the same alert in again on every replay.
                log.warn("Discarding alert with no alertId: station={} window=[{}, {})",
                        alert.getStationId(), alert.getWindowStart(), alert.getWindowEnd());
                continue;
            }
            byId.put(alert.getAlertId(), alert);
        }
        if (byId.isEmpty()) {
            return new PersistResult(Collections.emptySet(), alerts.size());
        }

        Set<String> existing = new HashSet<>(repository.findExistingIds(byId.keySet()));

        List<AlertEntity> toWrite = new ArrayList<>(byId.size());
        Set<String> insertedIds = new HashSet<>();

        for (Map.Entry<String, Alert> entry : byId.entrySet()) {
            String alertId = entry.getKey();
            Alert alert = entry.getValue();

            if (existing.contains(alertId)) {
                // Refresh the producer-owned fields: a re-emitted window may carry a corrected
                // value, and the row should reflect the latest view of it. The acknowledgement
                // state is left alone by the mapper.
                repository.findById(alertId).ifPresent(row -> {
                    mapper.updateEntity(row, alert);
                    toWrite.add(row);
                });
            } else {
                toWrite.add(mapper.toEntity(alert, receivedAt));
                insertedIds.add(alertId);
            }
        }

        repository.saveAll(toWrite);

        if (!existing.isEmpty()) {
            log.debug("Persisted batch: {} new, {} replayed", insertedIds.size(), existing.size());
        }
        return new PersistResult(insertedIds, alerts.size());
    }
}
