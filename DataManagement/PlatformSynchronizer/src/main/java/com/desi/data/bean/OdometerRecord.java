package com.desi.data.bean;

import com.desi.data.SensorRecord;
import com.desi.data.SensorUnit;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;

public class OdometerRecord implements SensorRecord {

    private final float value;

    private final String uuid;

    private final LocalDateTime dateTaken;

    public OdometerRecord(float value, String uuid, LocalDateTime dateTaken) {
        this.value = value;
        this.uuid = uuid;
        this.dateTaken = dateTaken;
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
