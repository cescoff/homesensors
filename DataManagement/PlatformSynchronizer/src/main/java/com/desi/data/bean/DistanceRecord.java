package com.desi.data.bean;

import com.desi.data.SensorRecord;
import com.desi.data.SensorUnit;
import org.joda.time.LocalDateTime;

public class DistanceRecord implements SensorRecord {

    private final String uuid;

    private final LocalDateTime dateTaken;

    private final float value;

    public DistanceRecord(String uuid, LocalDateTime dateTaken, float value) {
        this.uuid = uuid;
        this.dateTaken = dateTaken;
        this.value = value;
    }

    @Override
    public LocalDateTime getDateTaken() {
        return dateTaken;
    }

    @Override
    public float getValue() {
        return value;
    }

    @Override
    public String getSensorUUID() {
        return uuid;
    }

    @Override
    public SensorUnit getUnit() {
        return SensorUnit.KILOMETER;
    }
}
