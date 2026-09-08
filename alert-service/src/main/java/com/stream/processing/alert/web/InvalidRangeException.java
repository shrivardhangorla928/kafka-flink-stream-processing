package com.stream.processing.alert.web;

import java.time.Instant;

/** Raised when a caller supplies a time range whose end does not follow its start. HTTP 400. */
public class InvalidRangeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public InvalidRangeException(Instant from, Instant to) {
        super("Range end (" + to + ") must be strictly after range start (" + from + ")");
    }
}
