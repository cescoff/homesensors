package com.desi.sensors.data;

public enum SensorUnit {

    WATT(Energy.ELECTRIC, "W", "Watt"),
    DEGREES_CELSIUS(Energy.THERMAL, "°C", "Degrees Celsius");

    private final Energy energy;

    private final String numberDisplay;

    private final String label;

    SensorUnit(Energy energy, String numberDisplay, String label) {
        this.energy = energy;
        this.numberDisplay = numberDisplay;
        this.label = label;
    }

}
