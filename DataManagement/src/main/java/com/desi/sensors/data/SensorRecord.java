package com.desi.sensors.data;

import org.joda.time.LocalDateTime;

public interface SensorRecord {

    public LocalDateTime getDateTaken();

    public float getValue();

    public String getSensorUUID();

    public SensorUnit getUnit();

}
