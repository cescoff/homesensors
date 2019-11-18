package com.desi.sensors.data.bean;

import com.desi.sensors.data.SensorRecord;
import com.desi.sensors.data.SensorUnit;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;

public class TemperatureRecord implements SensorRecord {

    private final LocalDateTime dateTaken;

    private final float value;

    private final String sensorUUID;

    public TemperatureRecord(LocalDateTime dateTaken, float value, String sensorUUID) {
        this.dateTaken = dateTaken;
        this.value = value;
        this.sensorUUID = sensorUUID;
    }

    public LocalDateTime getDateTaken() {
        return dateTaken;
    }

    public float getValue() {
        return value;
    }

    public String getSensorUUID() {
        return sensorUUID;
    }

    public SensorUnit getUnit() {
        return SensorUnit.DEGREES_CELSIUS;
    }
}
