package com.stream.processing.ingest.web;

import com.stream.processing.common.SensorReading;
import com.stream.processing.common.Topics;
import com.stream.processing.ingest.config.IngestProperties;
import com.stream.processing.ingest.publish.TelemetryPublisher;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping(path = "/api/v1/telemetry", produces = MediaType.APPLICATION_JSON_VALUE)
public class TelemetryController {

    private final TelemetryPublisher publisher;
    private final IngestProperties properties;

    public TelemetryController(TelemetryPublisher publisher, IngestProperties properties) {
        this.publisher = publisher;
        this.properties = properties;
    }

    @PostMapping(path = "/readings", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PublishReceipt ingest(@Valid @RequestBody SensorReadingRequest request) {
        publisher.publish(request.toReading());
        return PublishReceipt.of(1, Topics.RAW_TELEMETRY);
    }

    @PostMapping(path = "/readings/batch", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PublishReceipt ingestBatch(@RequestBody List<@Valid SensorReadingRequest> requests) {
        if (requests == null || requests.isEmpty()) {
            throw new IllegalArgumentException("Batch must contain at least one reading");
        }
        if (requests.size() > properties.getMaxBatchSize()) {
            throw new IllegalArgumentException("Batch of " + requests.size()
                    + " exceeds stream.ingest.max-batch-size=" + properties.getMaxBatchSize());
        }
        List<SensorReading> readings = new ArrayList<>(requests.size());
        for (SensorReadingRequest request : requests) {
            readings.add(request.toReading());
        }
        int accepted = publisher.publishAll(readings);
        return PublishReceipt.of(accepted, Topics.RAW_TELEMETRY);
    }
}
