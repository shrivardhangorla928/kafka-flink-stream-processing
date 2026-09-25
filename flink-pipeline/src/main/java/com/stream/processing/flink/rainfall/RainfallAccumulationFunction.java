package com.stream.processing.flink.rainfall;

import com.stream.processing.common.Alert;
import com.stream.processing.common.SensorReading;
import com.stream.processing.common.SensorType;
import com.stream.processing.common.Severity;
import com.stream.processing.common.StationWindowAggregate;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Maintains a rolling 24-hour buffer of a station's rainfall readings, in keyed state, and
 * evaluates every one of {@link RainfallAlertPolicy#WINDOW_HOURS_ASCENDING} (1, 2, 3, 6, 12, 24h)
 * on every new reading.
 *
 * <p>Manual {@link MapState} instead of Flink's window API: the six windows are nested suffixes of
 * the same 24h lookback, so one buffer filtered in plain Java gives all six sums per event with a
 * single state read/write, versus six sliding-window operators re-buffering the same readings.
 * Keyed by event time so a repeated reading (retried scrape, overlapping poll) overwrites instead
 * of double-counting.</p>
 *
 * <p>Assumes the stream is already {@code keyBy(stationId)}'d; a non-rainfall reading is
 * defensively skipped rather than corrupting the buffer.</p>
 *
 * <p>Every incoming reading also emits one {@link StationWindowAggregate} on
 * {@link #HOURLY_AGGREGATE_TAG} for that single scrape, so rainfall still lands on the shared
 * aggregates topic/table downstream consumers read from.</p>
 */
public final class RainfallAccumulationFunction extends KeyedProcessFunction<String, SensorReading, Alert> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RainfallAccumulationFunction.class);

    private static final Duration BUFFER_RETENTION = Duration.ofHours(24);

    /** Side output carrying the single-reading hourly aggregate for the aggregates topic/table. */
    public static final OutputTag<StationWindowAggregate> HOURLY_AGGREGATE_TAG =
            new OutputTag<>("rainfall-hourly-aggregate") { };

    private transient MapState<Instant, SensorReading> bufferState;
    private transient RainfallAccumulationEvaluator evaluator;
    private transient Map<Severity, Counter> alertsBySeverity;

    @Override
    public void open(OpenContext openContext) {
        this.bufferState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("rainfall-last-24h", Instant.class, SensorReading.class));
        this.evaluator = new RainfallAccumulationEvaluator();
        this.alertsBySeverity = new EnumMap<>(Severity.class);
        for (Severity severity : Severity.values()) {
            alertsBySeverity.put(severity, getRuntimeContext().getMetricGroup()
                    .addGroup("severity", severity.name())
                    .counter("alertsEmitted"));
        }
    }

    @Override
    public void processElement(SensorReading reading, Context ctx, Collector<Alert> out) throws Exception {
        if (reading.getSensorType() != SensorType.RAINFALL) {
            LOG.warn("Skipping non-RAINFALL reading {} routed to RainfallAccumulationFunction for station {}",
                    reading.getSensorType(), reading.getStationId());
            return;
        }

        // put() keyed by eventTime: a repeat of the same timestamp overwrites instead of duplicating.
        bufferState.put(reading.getEventTime(), reading);

        Instant cutoff = reading.getEventTime().minus(BUFFER_RETENTION);
        List<SensorReading> retained = new ArrayList<>();
        List<Instant> expired = new ArrayList<>();
        for (Map.Entry<Instant, SensorReading> entry : bufferState.entries()) {
            if (entry.getKey().isBefore(cutoff)) {
                expired.add(entry.getKey());
            } else {
                retained.add(entry.getValue());
            }
        }
        for (Instant key : expired) {
            bufferState.remove(key);
        }

        Optional<Alert> alert = evaluator.evaluate(
                reading.getStationId(),
                reading.getStationName(),
                reading.getDistrictId(),
                reading.getLatitude(),
                reading.getLongitude(),
                retained,
                reading.getEventTime());

        if (alert.isPresent()) {
            Alert raised = alert.get();
            alertsBySeverity.get(raised.getSeverity()).inc();
            LOG.info("Raising {} alert {} for station {} ({} {} vs threshold {})",
                    raised.getSeverity(), raised.getAlertId(), raised.getStationId(),
                    raised.getObservedValue(), raised.getMetric(), raised.getThresholdValue());
            out.collect(raised);
        }

        ctx.output(HOURLY_AGGREGATE_TAG, buildHourlyAggregate(reading));
    }

    private static StationWindowAggregate buildHourlyAggregate(SensorReading reading) {
        StationWindowAggregate aggregate = new StationWindowAggregate();
        aggregate.setStationId(reading.getStationId());
        aggregate.setStationName(reading.getStationName());
        aggregate.setDistrictId(reading.getDistrictId());
        aggregate.setLatitude(reading.getLatitude());
        aggregate.setLongitude(reading.getLongitude());
        aggregate.setSensorType(SensorType.RAINFALL);
        aggregate.setUnit(reading.getUnit() != null ? reading.getUnit() : SensorType.RAINFALL.getUnit());
        aggregate.setWindowStart(reading.getEventTime().minus(1, ChronoUnit.HOURS));
        aggregate.setWindowEnd(reading.getEventTime());
        aggregate.setReadingCount(1L);
        aggregate.setSum(reading.getValue());
        aggregate.setMin(reading.getValue());
        aggregate.setMax(reading.getValue());
        aggregate.setAvg(reading.getValue());
        aggregate.setAggregatedValue(reading.getValue());
        aggregate.setComputedAt(Instant.now());
        return aggregate;
    }
}
