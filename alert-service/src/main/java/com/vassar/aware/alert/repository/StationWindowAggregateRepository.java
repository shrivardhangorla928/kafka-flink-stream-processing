package com.vassar.aware.alert.repository;

import com.vassar.aware.alert.domain.StationWindowAggregateEntity;
import com.vassar.aware.alert.domain.StationWindowKey;
import com.vassar.aware.common.SensorType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

/**
 * Access to the windowed-aggregate feed backing the per-station timeline endpoint.
 *
 * <p>Two derived finders rather than one JPQL query with a {@code :sensorType is null} guard:
 * Hibernate cannot always infer the type of a parameter that only ever appears in a null check,
 * and both variants hit the same {@code (station_id, window_start DESC)} index anyway.</p>
 */
public interface StationWindowAggregateRepository
        extends JpaRepository<StationWindowAggregateEntity, StationWindowKey> {

    List<StationWindowAggregateEntity> findByKeyStationIdOrderByKeyWindowStartDesc(
            String stationId, Pageable pageable);

    List<StationWindowAggregateEntity> findByKeyStationIdAndKeySensorTypeOrderByKeyWindowStartDesc(
            String stationId, SensorType sensorType, Pageable pageable);
}
