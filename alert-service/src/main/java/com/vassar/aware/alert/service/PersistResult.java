package com.vassar.aware.alert.service;

import java.util.Set;

/**
 * What one batch actually did to the store.
 *
 * <p>The consumer needs more than a row count: it must know <em>which</em> alerts were new, so
 * that the severity-tagged counter and the pipeline-latency histogram record replays as
 * duplicates instead of inflating the alert rate and skewing the latency distribution.</p>
 *
 * @param insertedIds ids that did not previously exist
 * @param received    records handed to the batch, including intra-batch repeats
 */
public record PersistResult(Set<String> insertedIds, int received) {

    public int inserted() {
        return insertedIds.size();
    }

    /** Everything received that did not turn into a new row: replays across polls and within one. */
    public int duplicates() {
        return received - insertedIds.size();
    }

    public boolean isNew(String alertId) {
        return insertedIds.contains(alertId);
    }
}
