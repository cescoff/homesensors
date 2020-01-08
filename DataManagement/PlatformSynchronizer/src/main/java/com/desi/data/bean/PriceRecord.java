package com.desi.data.bean;

import com.desi.data.SensorRecord;
import com.desi.data.SensorUnit;
import org.joda.time.LocalDateTime;

public class PriceRecord implements SensorRecord {

    private final LocalDateTime dateTaken;

    private final String uuid;

    private final float value;

    private final SensorUnit unit;

    public PriceRecord(LocalDateTime dateTaken, String uuid, float value, SensorUnit unit) {
        if (!unit.isCurrency()) {
            throw  new IllegalArgumentException("Unit '" + unit + "' is not a currency");
        }
        this.dateTaken = dateTaken;
        this.uuid = uuid;
        this.value = value;
        this.unit = unit;
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
        return unit;
    }
}
