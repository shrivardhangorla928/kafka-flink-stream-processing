package com.vassar.aware.ingest.web;

/**
 * Thrown when a burst is requested while {@code aware.ingest.simulator.enabled=false}.
 *
 * <p>The scraper bean does not exist in that mode, so the request cannot be served. It is a 503
 * rather than a 404: the endpoint exists and will work again once the simulator is switched back
 * on, which is exactly what "service unavailable" means.</p>
 */
public class SimulatorDisabledException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SimulatorDisabledException(String message) {
        super(message);
    }
}
