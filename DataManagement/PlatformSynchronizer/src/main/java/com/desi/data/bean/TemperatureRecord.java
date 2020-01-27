package com.desi.data.bean;

import com.desi.data.SensorRecord;
import com.desi.data.SensorType;
import com.desi.data.SensorUnit;
import org.joda.time.LocalDateTime;

public class TemperatureRecord implements SensorRecord {

    private final LocalDateTime dateTaken;

    private final float value;

    private final String sensorUUID;

    private final SensorType sensorType;

    public TemperatureRecord(LocalDateTime dateTaken, float value, String sensorUUID, SensorType sensorType) {
        this.dateTaken = dateTaken;
        this.value = value;
        this.sensorUUID = sensorUUID;
        this.sensorType = sensorType;
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

    @Override
    public SensorType getType() {
        return sensorType;
    }
}
