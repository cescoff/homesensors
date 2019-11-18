package com.desi.data;

import org.joda.time.LocalDateTime;

public interface AggregatedSensorRecord {

    public LocalDateTime getPeriodBegin();

    public LocalDateTime getPeriodEnd();

    public Iterable<String> getSensorUUIDs();

    public String getDisplayName(final String uuid);

    public SensorUnit getUnit(final String uuid);

    public float getSensorValue(final String uuid);

    public boolean addValue(final SensorRecord record);

}
