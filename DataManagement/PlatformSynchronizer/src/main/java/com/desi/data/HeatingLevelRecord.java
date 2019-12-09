package com.desi.data;


import com.desi.data.bean.TimePeriod;
import org.joda.time.LocalDateTime;

public interface HeatingLevelRecord {

    public LocalDateTime getDateTime();

    public float getHeatingLevel();

    public float getIndoorMonitorValue();

    public float getOutdoorMonitorValue();

    public float getHeatingMonitorValue();

    public boolean ready();

    public boolean addHeating(final LocalDateTime dateTime, final float monitorValue);

    public boolean addIndoor(final LocalDateTime dateTime, final float monitorValue);

    public boolean addOutdoor(final LocalDateTime dateTime, final float monitorValue);

    public TimePeriod getPerdiod();

}
