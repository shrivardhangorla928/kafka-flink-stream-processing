package com.stream.processing.ingest.scrape;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.OptionalDouble;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class StationRainState {

    private record Seen(Instant time, double rain) {
    }

    private final ConcurrentHashMap<Long, Seen> lastSeen = new ConcurrentHashMap<>();

    public OptionalDouble incrementIfChanged(long clientId, Instant newTime, double newRain) {
        Seen prior = lastSeen.get(clientId);
        if (prior == null) {
            lastSeen.put(clientId, new Seen(newTime, newRain));
            return OptionalDouble.of(newRain);
        }
        if (prior.time().equals(newTime)) {
            return OptionalDouble.empty();
        }
        double increment = newRain >= prior.rain() ? newRain - prior.rain() : newRain;
        lastSeen.put(clientId, new Seen(newTime, newRain));
        return OptionalDouble.of(increment);
    }
}
