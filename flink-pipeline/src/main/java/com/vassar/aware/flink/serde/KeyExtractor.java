package com.vassar.aware.flink.serde;

import java.io.Serializable;

/**
 * Pulls the Kafka message key out of an event.
 *
 * <p>Declared {@link Serializable} so a method reference such as {@code Alert::getStationId} can
 * be handed straight to {@link StringKeySerializationSchema} and still ship to the task managers.
 * A plain {@code java.util.function.Function} lambda would not survive that trip.</p>
 *
 * @param <T> event type the key is taken from
 */
@FunctionalInterface
public interface KeyExtractor<T> extends Serializable {

    /** The partition key for the event, or {@code null} when the event has no natural key. */
    String keyOf(T value);
}
