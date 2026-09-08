package com.stream.processing.alert.web;

/** Raised when a caller addresses an alert id that is not in the store. Mapped to HTTP 404. */
public class AlertNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String alertId;

    public AlertNotFoundException(String alertId) {
        super("No alert with id " + alertId);
        this.alertId = alertId;
    }

    public String getAlertId() {
        return alertId;
    }
}
