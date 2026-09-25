package com.stream.processing.ingest.scrape;

import com.stream.processing.common.SensorReading;
import com.stream.processing.common.SensorType;
import com.stream.processing.ingest.publish.TelemetryPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.OptionalDouble;

@Component
@ConditionalOnProperty(prefix = "stream.ingest", name = "desweather-scrape-enabled",
        havingValue = "true", matchIfMissing = true)
public class TelemetryScraper {

    private static final Logger LOG = LoggerFactory.getLogger(TelemetryScraper.class);

    private static final DateTimeFormatter LDATE_FORMAT =
            DateTimeFormatter.ofPattern("dd/MM/yyyy hh:mm:ss a", Locale.ENGLISH);
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final String SOURCE = "DESWEATHER_AP";

    private final DesweatherClient desweatherClient;
    private final StationRainState stationRainState;
    private final TelemetryPublisher publisher;

    public TelemetryScraper(DesweatherClient desweatherClient, StationRainState stationRainState,
                             TelemetryPublisher publisher) {
        this.desweatherClient = desweatherClient;
        this.stationRainState = stationRainState;
        this.publisher = publisher;
    }

    @Scheduled(fixedRateString = "${stream.ingest.desweather-poll-interval-ms:300000}")
    public void scheduledScrape() {
        List<DesweatherReading> stations = desweatherClient.fetch();
        if (stations.isEmpty()) {
            LOG.warn("Desweather feed returned no stations");
            return;
        }

        List<SensorReading> readings = new ArrayList<>();
        for (DesweatherReading r : stations) {
            Instant eventTime = parseLdate(r.getLdate());
            if (eventTime == null) {
                continue;
            }
            OptionalDouble incrementOpt = stationRainState.incrementIfChanged(r.getClientid(), eventTime, r.getRain());
            if (incrementOpt.isEmpty()) {
                continue;
            }
            readings.add(toSensorReading(r, eventTime, incrementOpt.getAsDouble()));
        }

        publisher.publishAll(readings);
        LOG.info("Scraped {} of {} stations with new data", readings.size(), stations.size());
    }

    private static SensorReading toSensorReading(DesweatherReading r, Instant eventTime, double increment) {
        SensorReading reading = new SensorReading();
        reading.setStationId(String.valueOf(r.getClientid()));
        reading.setStationName(r.getLocation());
        reading.setDistrictId(r.getDistrict());
        reading.setSensorType(SensorType.RAINFALL);
        reading.setValue(increment);
        reading.setUnit(SensorType.RAINFALL.getUnit());
        reading.setLatitude(r.getLatitude());
        reading.setLongitude(r.getLongitude());
        reading.setEventTime(eventTime);
        reading.setSource(SOURCE);
        return reading;
    }

    private static Instant parseLdate(String ldate) {
        if (ldate == null || ldate.isBlank()) {
            return null;
        }
        try {
            return java.time.LocalDateTime.parse(ldate, LDATE_FORMAT).atZone(IST).toInstant();
        } catch (DateTimeParseException e) {
            LOG.warn("Unparseable ldate '{}': {}", ldate, e.getMessage());
            return null;
        }
    }
}
