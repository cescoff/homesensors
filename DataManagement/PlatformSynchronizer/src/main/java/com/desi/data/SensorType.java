package com.desi.data;


import com.google.common.base.Optional;

public enum SensorType {

    INDOOR_TEMPERATURE(0),
    OUTDOOR_TEMPERATURE(1),
    HEATING_TEMPERATURE(2),
    HEATER_TEMPERATURE(3),
    DISTANCE_ODOMETER(4),
    GASOLINE_VOLUME_ODOMETER(5),
    GAS_VOLUME_ODOMETER(6),
    DISTANCE(7),
    POSITION(8),
    PRICE(9);

    private final int id;

    private SensorType(int id) {
        this.id = id;
    }

    public int id() {
        return this.id;
    }

    public static Optional<SensorType> resolve(int id) {
        for (SensorType sensorType : values()) {
            if (sensorType.id == id) {
                return Optional.of(sensorType);
            }
        }
        return Optional.absent();
    }

}
