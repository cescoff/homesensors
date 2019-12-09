package com.desi.data.bean;

import com.desi.data.HeatingLevelRecord;
import com.desi.data.utils.HeatingLevelHelper;
import com.google.common.base.Predicate;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.LocalDateTime;


public class DefaultHeatingLevelRecord implements HeatingLevelRecord {

    private final LocalDateTime dateTime;

    private float heatingMonitorValue = -127;

    private float indoorMonitorValue = -127;

    private float outdoorMonitorValue = -127;

    public DefaultHeatingLevelRecord(LocalDateTime dateTime) {
        this.dateTime = dateTime;
    }

    @Override
    public LocalDateTime getDateTime() {
        return this.dateTime;
    }

    @Override
    public float getHeatingLevel() {
        return HeatingLevelHelper.getLevel(dateTime, heatingMonitorValue, indoorMonitorValue, outdoorMonitorValue);
    }

    @Override
    public float getIndoorMonitorValue() {
        return indoorMonitorValue;
    }

    @Override
    public float getOutdoorMonitorValue() {
        return outdoorMonitorValue;
    }

    @Override
    public float getHeatingMonitorValue() {
        return heatingMonitorValue;
    }

    @Override
    public boolean ready() {
        return dateTime != null && heatingMonitorValue > -127 && indoorMonitorValue > -127 && outdoorMonitorValue > -127;
    }

    @Override
    public boolean addHeating(LocalDateTime dateTime, float monitorValue) {
        if (!this.dateTime.equals(dateTime)) {
            return false;
        }
        if (heatingMonitorValue > -127) {
            return false;
        }
        this.heatingMonitorValue = monitorValue;
        return true;
    }

    @Override
    public boolean addIndoor(LocalDateTime dateTime, float monitorValue) {
        if (!this.dateTime.equals(dateTime)) {
            return false;
        }
        if (indoorMonitorValue > -127) {
            return false;
        }
        this.indoorMonitorValue = monitorValue;
        return true;
    }

    @Override
    public boolean addOutdoor(LocalDateTime dateTime, float monitorValue) {
        if (!this.dateTime.equals(dateTime)) {
            return false;
        }
        if (outdoorMonitorValue > -127) {
            return false;
        }
        this.outdoorMonitorValue = monitorValue;
        return true;
    }

    private void check() {
        if (dateTime == null) {
            throw new IllegalStateException("Unready record date time is null");
        }
        if (heatingMonitorValue <= -127 || indoorMonitorValue <= -127 || outdoorMonitorValue <= -127) {
            throw new IllegalStateException("Unready record missing monitor value");
        }
    }

    @Override
    public TimePeriod getPerdiod() {
        check();

        if (dateTime.getHourOfDay() >= 0 && dateTime.getHourOfDay() <= 7) {
            return TimePeriod.Night;
        }

        return TimePeriod.Day;
    }

    public static Predicate<HeatingLevelRecord> ReadyFilter() {
        return new Predicate<HeatingLevelRecord>() {
            @Override
            public boolean apply(@Nullable HeatingLevelRecord heatingLevelRecord) {
                return heatingLevelRecord.ready();
            }

            @Override
            public boolean test(@Nullable HeatingLevelRecord input) {
                return apply(input);
            }
        };
    }

}
