package com.desi.data.bean;

import com.desi.data.SensorRecord;
import com.desi.data.SensorUnit;
import org.joda.time.LocalDateTime;

public class UnreadableGasolineVolumeRecord implements SensorRecord {

    private final LocalDateTime dateTaken;

    private final String uuid;

    private final String rawTexts;

    public UnreadableGasolineVolumeRecord(LocalDateTime dateTaken, String uuid, String texts) {
        this.dateTaken = dateTaken;
        this.uuid = uuid;
        this.rawTexts = texts;
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
        return SensorUnit.UNREADABLE_GASOLINE_VOLUME;
    }

    public String getRawTexts() {
        return rawTexts;
    }
}
