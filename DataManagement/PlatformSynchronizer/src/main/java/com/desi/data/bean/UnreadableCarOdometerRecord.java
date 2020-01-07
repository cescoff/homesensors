package com.desi.data.bean;

import com.desi.data.SensorRecord;
import com.desi.data.SensorUnit;
import org.joda.time.LocalDateTime;

public class UnreadableCarOdometerRecord implements SensorRecord {

    private final LocalDateTime dateTaken;

    private final String uuid;

    public UnreadableCarOdometerRecord(LocalDateTime dateTaken, String uuid) {
        this.dateTaken = dateTaken;
        this.uuid = uuid;
    }

    @Override
    public LocalDateTime getDateTaken() {
        return dateTaken;
    }

    @Override
    public float getValue() {
        return 0;
    }

    @Override
    public String getSensorUUID() {
        return uuid;
    }

    @Override
    public SensorUnit getUnit() {
        return SensorUnit.UNREADABLE_ODOMETER;
    }
}
