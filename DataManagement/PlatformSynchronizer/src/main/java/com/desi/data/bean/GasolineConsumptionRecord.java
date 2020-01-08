package com.desi.data.bean;

import com.desi.data.SensorRecord;
import com.desi.data.SensorUnit;
import org.joda.time.LocalDateTime;

import static com.desi.data.SensorUnit.GASOLINE_CONSUMPTION;

public class GasolineConsumptionRecord implements SensorRecord {

    private final LocalDateTime dateTaken;

    private final float value;

    private final String uuid;

    public GasolineConsumptionRecord(LocalDateTime dateTaken, float value, String uuid) {
        this.dateTaken = dateTaken;
        this.value = value;
        this.uuid = uuid;
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
        return GASOLINE_CONSUMPTION;
    }
}
