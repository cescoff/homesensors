package com.desi.data;

public enum SensorUnit {

    UNREADABLE_ODOMETER(Energy.FOSSIL, "unreadable", "unreadable"),
    UNREADABLE_GASOLINE_VOLUME(Energy.FOSSIL, "unreadable", "unreadable"),
    CUBIC_METER(Energy.FOSSIL, "m3", "Cubic Meter"),
    KILOMETER(Energy.FOSSIL, "km", "Kilometer"),
    GASOLINE_VOLUME(Energy.FOSSIL, "L", "Litres"),
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

    public String getDisplay() {
        return numberDisplay;
    }

}
