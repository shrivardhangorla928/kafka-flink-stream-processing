package com.vassar.aware.alert.service;

import com.vassar.aware.alert.domain.StationWindowAggregateEntity;
import com.vassar.aware.alert.domain.StationWindowKey;
import com.vassar.aware.alert.mapper.StationWindowAggregateMapper;
import com.vassar.aware.alert.repository.StationWindowAggregateRepository;
import com.vassar.aware.common.StationWindowAggregate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Idempotent write path for the windowed-aggregate observability feed.
 *
 * <p>Same replay problem as the alert path, solved with the same read-then-write shape, but
 * keyed on the natural key {@code (stationId, sensorType, windowStart)} because aggregates carry
 * no synthetic id of their own.</p>
 */
@Service
public class AggregatePersistenceService {

    private static final Logger log = LoggerFactory.getLogger(AggregatePersistenceService.class);

    private final StationWindowAggregateRepository repository;
    private final StationWindowAggregateMapper mapper;

    public AggregatePersistenceService(StationWindowAggregateRepository repository,
                                       StationWindowAggregateMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    /**
     * Upserts a batch of windows in one transaction.
     *
     * @return the number of rows written, after intra-batch repeats have been collapsed
     */
    @Transactional
    public int persist(List<StationWindowAggregate> aggregates, Instant receivedAt) {
        if (aggregates.isEmpty()) {
            return 0;
        }

        Map<StationWindowKey, StationWindowAggregate> byKey = new LinkedHashMap<>();
        for (StationWindowAggregate aggregate : aggregates) {
            if (aggregate.getStationId() == null
                    || aggregate.getSensorType() == null
                    || aggregate.getWindowStart() == null) {
                log.warn("Discarding aggregate with incomplete natural key: {}", aggregate);
                continue;
            }
            byKey.put(mapper.keyOf(aggregate), aggregate);
        }
        if (byKey.isEmpty()) {
            return 0;
        }

        List<StationWindowAggregateEntity> toWrite = new ArrayList<>(byKey.size());
        for (Map.Entry<StationWindowKey, StationWindowAggregate> entry : byKey.entrySet()) {
            StationWindowAggregateEntity row = repository.findById(entry.getKey()).orElse(null);
            if (row == null) {
                toWrite.add(mapper.toEntity(entry.getValue(), receivedAt));
            } else {
                mapper.updateEntity(row, entry.getValue(), receivedAt);
                toWrite.add(row);
            }
        }

        repository.saveAll(toWrite);
        return toWrite.size();
    }
}
