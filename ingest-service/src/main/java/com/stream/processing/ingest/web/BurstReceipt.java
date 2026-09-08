package com.stream.processing.ingest.web;

import java.time.Instant;

/**
 * Body of a burst response.
 *
 * <p>{@code activeStormEpisodes} is returned so that a demonstrator can see whether the forced
 * round will actually breach a threshold, instead of publishing a burst and then wondering why no
 * alert appeared.</p>
 *
 * @param published           readings handed to the producer by the forced round
 * @param requestedStorms     storm stations asked for on the query string
 * @param activeStormEpisodes storm episodes in flight after the request was applied
 * @param publishedAt         server time the round ran
 */
public record BurstReceipt(int published, int requestedStorms, int activeStormEpisodes, Instant publishedAt) {
}
