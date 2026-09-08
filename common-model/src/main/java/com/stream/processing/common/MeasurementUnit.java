package com.stream.processing.common;

/** Unit of the numeric value carried by a reading, an aggregate or an alert. */
public enum MeasurementUnit {

    MM("mm"),
    METRE("m"),
    PERCENT("%");

    private final String symbol;

    MeasurementUnit(String symbol) {
        this.symbol = symbol;
    }

    public String getSymbol() {
        return symbol;
    }
}
