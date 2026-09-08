package com.stream.processing.ingest.config;

import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Topic geometry applied when the service declares the pipeline's topics, bound from
 * {@code stream.kafka.*}.
 *
 * <p>Partition count is a property rather than a constant because it caps the parallelism of
 * every downstream Flink operator keyed by station: raising it is the first lever pulled when
 * the evaluation chapter scales the pipeline out. Replication factor is separate because a
 * single-broker developer machine cannot satisfy the value used in the Kubernetes deployment.</p>
 */
@Validated
@ConfigurationProperties(prefix = "stream.kafka")
public class KafkaTopicProperties {

    @Min(1)
    private int partitions = 3;

    @Min(1)
    private short replicationFactor = 1;

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public short getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(short replicationFactor) {
        this.replicationFactor = replicationFactor;
    }
}
