package com.stream.processing.flink.alerting;

import com.stream.processing.common.Alert;
import com.stream.processing.common.Severity;
import com.stream.processing.common.StationWindowAggregate;
import com.stream.processing.flink.config.ThresholdRuleSet;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The operator wrapper around {@link ThresholdEvaluator}: zero or one alert per window aggregate.
 *
 * <p>Holds the policy as a serializable field rather than reading a static constant, so the bands
 * travel with the job graph and a resubmission with different {@code --threshold.*} arguments takes
 * effect without a rebuild.</p>
 *
 * <p>A counter is registered per severity rather than one counter with a label because Flink's
 * metric groups are the dimension mechanism, and the Prometheus reporter turns a per-severity
 * subgroup into exactly the {@code severity="SEVERE"} label the Grafana panels group by.</p>
 */
public final class ThresholdEvaluationFunction extends ProcessFunction<StationWindowAggregate, Alert> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ThresholdEvaluationFunction.class);

    private final ThresholdRuleSet rules;

    private transient ThresholdEvaluator evaluator;
    private transient Map<Severity, Counter> alertsBySeverity;

    public ThresholdEvaluationFunction(ThresholdRuleSet rules) {
        this.rules = Objects.requireNonNull(rules, "rules");
    }

    @Override
    public void open(OpenContext openContext) {
        this.evaluator = new ThresholdEvaluator(rules);
        this.alertsBySeverity = new EnumMap<>(Severity.class);
        for (Severity severity : Severity.values()) {
            alertsBySeverity.put(severity, getRuntimeContext().getMetricGroup()
                    .addGroup("severity", severity.name())
                    .counter("alertsEmitted"));
        }
    }

    @Override
    public void processElement(StationWindowAggregate aggregate, Context ctx, Collector<Alert> out) {
        Optional<Alert> alert = evaluator.evaluate(aggregate);
        if (alert.isEmpty()) {
            return;
        }
        Alert raised = alert.get();
        alertsBySeverity.get(raised.getSeverity()).inc();
        LOG.info("Raising {} alert {} for station {} ({} {} vs threshold {})",
                raised.getSeverity(), raised.getAlertId(), raised.getStationId(),
                raised.getObservedValue(), raised.getMetric(), raised.getThresholdValue());
        out.collect(raised);
    }
}
