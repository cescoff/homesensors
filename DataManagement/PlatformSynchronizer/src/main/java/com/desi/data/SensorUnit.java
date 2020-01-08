package com.desi.data;

import com.google.common.base.Optional;
import org.apache.commons.lang.StringUtils;

public enum SensorUnit {

    GASOLINE_CONSUMPTION(Energy.FOSSIL, "L/100", "Litres au 100 km"),
    EURO(Energy.ECONOMIC, "€", "Euro"),
    DOLLAR(Energy.ECONOMIC, "$", "Dollar"),
    POUND(Energy.ECONOMIC, "£", "Pound"),
    UNREADABLE_ODOMETER(Energy.FOSSIL, "unreadable", "unreadable"),
    UNREADABLE_GASOLINE_VOLUME(Energy.FOSSIL, "unreadable", "unreadable"),
    CUBIC_METER(Energy.FOSSIL, "m3", "Cubic Meter"),
    KILOMETER(Energy.FOSSIL, "km", "Kilometer"),
    GASOLINE_VOLUME(Energy.FOSSIL, "L", "Litres"),
    WATT(Energy.ELECTRIC, "W", "Watt"),
    POSITION(Energy.DISTANCE, "Lat/Long", "Latitude/Longitude"),
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

    public boolean isCurrency() {
        return this == DOLLAR || this == EURO || this == POUND;
    }

    public static Optional<SensorUnit> getCurrency(final String text) {
        if (StringUtils.contains(text, '$')) {
            return Optional.of(DOLLAR);
        }
        if (StringUtils.contains(text, '€') || StringUtils.containsIgnoreCase(text, " E ")) {
            return Optional.of(EURO);
        }
        if (StringUtils.contains(text, '£')) {
            return Optional.of(POUND);
        }
        return Optional.absent();
    }
}
