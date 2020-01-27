package com.desi.data.bean;

import com.desi.data.SensorRecord;
import com.desi.data.SensorType;
import com.desi.data.SensorUnit;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.joda.time.LocalDateTime;

import java.util.List;

public class UnreadableGasolineVolumeRecord implements SensorRecord {

    private final LocalDateTime dateTaken;

    private final String uuid;

    private final String fileName;

    private final List<String> annotatedTexts = Lists.newArrayList();

    private final String reason;

    public UnreadableGasolineVolumeRecord(LocalDateTime dateTaken, String uuid, String fileName, String reason) {
        this.dateTaken = dateTaken;
        this.uuid = uuid;
        this.fileName = fileName;
        this.reason = reason;
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

    public void addTexts(final Iterable<String> texts) {
        Iterables.addAll(this.annotatedTexts, texts);
    }

    public String getFileName() {
        return fileName;
    }

    public List<String> getAnnotatedTexts() {
        return annotatedTexts;
    }

    public String getUuid() {
        return uuid;
    }

    public String getReason() {
        return reason;
    }

    @Override
    public SensorType getType() {
        return SensorType.GASOLINE_VOLUME_ODOMETER;
    }
}
