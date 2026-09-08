package com.vassar.aware.flink.aggregation;

import com.vassar.aware.common.StationWindowAggregate;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;

/**
 * Stamps the fired window's bounds onto the accumulated result and emits the published aggregate.
 *
 * <p>Paired with {@link ReadingAggregateFunction} in the two-argument {@code aggregate(...)} form:
 * the aggregate function does the incremental reduction, and this one runs exactly once per fired
 * window, which is the only point where the window bounds are actually knowable. Doing the whole
 * job in a process function instead would mean buffering every reading.</p>
 */
public final class WindowStampingFunction
        extends ProcessWindowFunction<StationWindowAccumulator, StationWindowAggregate, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    private transient Counter windowsFired;

    @Override
    public void open(OpenContext openContext) {
        this.windowsFired = getRuntimeContext().getMetricGroup().counter("windowsFired");
    }

    @Override
    public void process(String stationId,
                        Context context,
                        Iterable<StationWindowAccumulator> accumulators,
                        Collector<StationWindowAggregate> out) {
        for (StationWindowAccumulator accumulator : accumulators) {
            // The station id from the key is authoritative: it is what the window was grouped on,
            // whereas the accumulator's copy came from whichever reading happened to be last.
            accumulator.setStationId(stationId);
            windowsFired.inc();
            out.collect(WindowAggregates.toAggregate(
                    accumulator,
                    Instant.ofEpochMilli(context.window().getStart()),
                    Instant.ofEpochMilli(context.window().getEnd()),
                    Instant.now()));
        }
    }
}
