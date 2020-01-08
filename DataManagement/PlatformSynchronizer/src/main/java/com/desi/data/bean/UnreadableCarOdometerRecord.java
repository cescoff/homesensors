package com.desi.data.bean;

import com.desi.data.SensorRecord;
import com.desi.data.SensorUnit;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.joda.time.LocalDateTime;

import java.util.List;

public class UnreadableCarOdometerRecord implements SensorRecord {

    private final LocalDateTime dateTaken;

    private final String uuid;

    private final String fileName;

    private final List<String> annotatedTexts = Lists.newArrayList();

    public UnreadableCarOdometerRecord(LocalDateTime dateTaken, String uuid, String fileName) {
        this.dateTaken = dateTaken;
        this.uuid = uuid;
        this.fileName = fileName;
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

    public void addTexts(final Iterable<String> texts) {
        Iterables.addAll(this.annotatedTexts, texts);
    }

    public List<String> getAnnotatedTexts() {
        return annotatedTexts;
    }

    public String getFileName() {
        return fileName;
    }
}
