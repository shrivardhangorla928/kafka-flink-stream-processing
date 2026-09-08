package com.vassar.aware.alert.mapper;

import com.vassar.aware.alert.domain.StationWindowAggregateEntity;
import com.vassar.aware.alert.domain.StationWindowKey;
import com.vassar.aware.alert.web.dto.StationWindowDto;
import com.vassar.aware.common.StationWindowAggregate;
import org.springframework.stereotype.Component;

import java.time.Instant;

/** Translates windowed aggregates between the wire event, the row and the response body. */
@Component
public class StationWindowAggregateMapper {

    /** The natural key an inbound aggregate upserts on. */
    public StationWindowKey keyOf(StationWindowAggregate aggregate) {
        return new StationWindowKey(
                aggregate.getStationId(), aggregate.getSensorType(), aggregate.getWindowStart());
    }

    public StationWindowAggregateEntity toEntity(StationWindowAggregate aggregate, Instant receivedAt) {
        StationWindowAggregateEntity entity = new StationWindowAggregateEntity(keyOf(aggregate));
        copyEventFields(aggregate, entity);
        entity.setReceivedAt(receivedAt);
        return entity;
    }

    /**
     * Refreshes an existing window from a replay. {@code receivedAt} is advanced here - unlike on
     * alerts - because for the observability feed the useful question is "when did the sink last
     * see this window", and a replayed aggregate carries no operator state that could be lost.
     */
    public void updateEntity(StationWindowAggregateEntity entity,
                             StationWindowAggregate aggregate,
                             Instant receivedAt) {
        copyEventFields(aggregate, entity);
        entity.setReceivedAt(receivedAt);
    }

    private void copyEventFields(StationWindowAggregate aggregate, StationWindowAggregateEntity entity) {
        entity.setWindowEnd(aggregate.getWindowEnd() == null
                ? aggregate.getWindowStart()
                : aggregate.getWindowEnd());
        entity.setStationName(aggregate.getStationName());
        entity.setDistrictId(aggregate.getDistrictId());
        entity.setUnit(aggregate.getUnit());
        entity.setReadingCount(aggregate.getReadingCount());
        entity.setSum(aggregate.getSum());
        entity.setMin(aggregate.getMin());
        entity.setMax(aggregate.getMax());
        entity.setAvg(aggregate.getAvg());
        entity.setAggregatedValue(aggregate.getAggregatedValue());
        entity.setLatitude(aggregate.getLatitude());
        entity.setLongitude(aggregate.getLongitude());
        entity.setComputedAt(aggregate.getComputedAt());
    }

    public StationWindowDto toDto(StationWindowAggregateEntity entity) {
        return new StationWindowDto(
                entity.getKey().getStationId(),
                entity.getStationName(),
                entity.getDistrictId(),
                entity.getKey().getSensorType(),
                entity.getUnit(),
                entity.getKey().getWindowStart(),
                entity.getWindowEnd(),
                entity.getReadingCount(),
                entity.getSum(),
                entity.getMin(),
                entity.getMax(),
                entity.getAvg(),
                entity.getAggregatedValue(),
                entity.getComputedAt(),
                entity.getReceivedAt());
    }
}
