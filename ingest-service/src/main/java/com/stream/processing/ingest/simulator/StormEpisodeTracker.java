package com.stream.processing.ingest.simulator;

import com.stream.processing.common.SensorType;
import com.stream.processing.ingest.station.Station;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Decides which stations are currently living through a storm episode and for how much longer.
 *
 * <p>Split out of {@link ReadingSimulator} because it is the only genuinely stateful part of the
 * simulation and the one whose behaviour the evaluation chapter depends on: if episodes are too
 * rare the demo produces no alerts, and if they never end every window alerts and the pipeline's
 * selectivity cannot be measured. It has no Spring or Kafka dependency so both properties can be
 * asserted directly.</p>
 *
 * <p>The episode start probability is derived from the configured steady-state fraction rather
 * than configured separately, because the two cannot be set independently without contradicting
 * each other. A station alternates between idle and storming, so the long-run fraction storming
 * is {@code E[storm] / (E[idle] + E[storm])}; with geometric idle periods of mean {@code 1/p}
 * this gives {@code p = f / ((1 - f) * meanCycles)}. With the defaults (f = 0.12, mean 7.5
 * cycles) that is about 0.018 per station per cycle, so roughly six of fifty stations are
 * storming at any moment.</p>
 *
 * <p>Not thread-safe: it is driven by the single scheduled scrape thread, and burst requests are
 * serialised onto the same round through {@link ReadingSimulator}.</p>
 */
public final class StormEpisodeTracker {

    private final SimulatorSettings settings;
    private final Random random;

    /** station id to remaining cycles; absence means the station is behaving normally. */
    private final Map<String, Integer> remainingCycles = new HashMap<>();

    private boolean primed;

    public StormEpisodeTracker(SimulatorSettings settings, Random random) {
        this.settings = settings;
        this.random = random;
    }

    /**
     * Moves the tracker on by one scrape cycle: expires finished episodes and rolls for new ones.
     *
     * <p>The very first call primes instead of rolling. Waiting for the random walk to produce
     * the first episode would take about fifty cycles - nearly an hour at the default scrape
     * interval - and a demo cannot start with an hour of nothing happening.</p>
     */
    public void advance(List<Station> stations) {
        if (!primed) {
            primeInitialEpisodes(stations);
            primed = true;
            return;
        }
        expireFinishedEpisodes();
        double startProbability = episodeStartProbability();
        for (Station station : stations) {
            if (!remainingCycles.containsKey(station.stationId()) && random.nextDouble() < startProbability) {
                remainingCycles.put(station.stationId(), drawDuration());
            }
        }
    }

    /**
     * Starts episodes on up to {@code count} stations that are not already storming, so a demo can
     * provoke alerts without waiting for the scheduled roll of the dice.
     *
     * @return the number of episodes actually started, which is lower than {@code count} when
     *         nearly every station is already storming
     */
    public int forceEpisodes(List<Station> stations, int count) {
        if (count <= 0) {
            return 0;
        }
        List<Station> candidates = new ArrayList<>(stations.stream()
                .filter(station -> !remainingCycles.containsKey(station.stationId()))
                .toList());
        Collections.shuffle(candidates, random);

        int started = 0;
        for (Station station : candidates) {
            if (started == count) {
                break;
            }
            remainingCycles.put(station.stationId(), drawDuration());
            started++;
        }
        return started;
    }

    public boolean isStorming(String stationId) {
        return remainingCycles.containsKey(stationId);
    }

    public int activeEpisodes() {
        return remainingCycles.size();
    }

    /** Remaining cycles per storming station, for diagnostics and assertions. */
    public Map<String, Integer> snapshot() {
        return Map.copyOf(remainingCycles);
    }

    private void expireFinishedEpisodes() {
        Iterator<Map.Entry<String, Integer>> it = remainingCycles.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Integer> entry = it.next();
            int left = entry.getValue() - 1;
            if (left <= 0) {
                it.remove();
            } else {
                entry.setValue(left);
            }
        }
    }

    /**
     * Seeds the first episodes stratified by sensor type, so that the opening cycles exercise the
     * rainfall, reservoir and river threshold sets rather than whichever type the shuffle happened
     * to favour.
     */
    private void primeInitialEpisodes(List<Station> stations) {
        if (settings.stormFraction() <= 0.0d) {
            return;
        }
        Map<SensorType, List<Station>> byType = new LinkedHashMap<>();
        for (Station station : stations) {
            byType.computeIfAbsent(station.sensorType(), key -> new ArrayList<>()).add(station);
        }
        for (List<Station> group : byType.values()) {
            int quota = Math.max(1, (int) Math.round(settings.stormFraction() * group.size()));
            quota = Math.min(quota, group.size());
            List<Station> shuffled = new ArrayList<>(group);
            Collections.shuffle(shuffled, random);
            for (int i = 0; i < quota; i++) {
                remainingCycles.put(shuffled.get(i).stationId(), drawDuration());
            }
        }
    }

    private double episodeStartProbability() {
        double fraction = settings.stormFraction();
        if (fraction <= 0.0d) {
            return 0.0d;
        }
        if (fraction >= 1.0d) {
            return 1.0d;
        }
        return fraction / ((1.0d - fraction) * settings.meanStormCycles());
    }

    private int drawDuration() {
        int span = settings.stormMaxCycles() - settings.stormMinCycles() + 1;
        return settings.stormMinCycles() + random.nextInt(span);
    }
}
