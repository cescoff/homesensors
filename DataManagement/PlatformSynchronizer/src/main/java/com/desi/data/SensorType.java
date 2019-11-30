package com.desi.data;

public enum SensorType {

    INDOOR(0),
    OUTDOOR(1),
    HEATING(2),
    HEATER(3);

    private final int id;

    private SensorType(int id) {
        this.id = id;
    }

    public int id() {
        return this.id;
    }

}
