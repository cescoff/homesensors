package com.desi.data.bean;

import com.desi.data.SensorRecord;
import com.desi.data.SensorType;
import com.desi.data.SensorUnit;
import org.joda.time.LocalDateTime;

public class HeatBurnSensorRecord implements SensorRecord {

    private final String uuid;

    private final TemperatureRecord periodBegin;

    private final TemperatureRecord periodEnd;

    public HeatBurnSensorRecord(String uuid, TemperatureRecord periodBegin, TemperatureRecord periodEnd) {
        this.uuid = uuid;
        this.periodBegin = periodBegin;
        this.periodEnd = periodEnd;
    }

    @Override
    public LocalDateTime getDateTaken() {
        return periodEnd.getDateTaken();
    }

    @Override
    public float getValue() {
        if (!periodEnd.getSensorUUID().equals(periodBegin.getSensorUUID())) {
            return 0;
        }
        if (periodEnd.getValue() <= periodBegin.getValue()) {
            return 0;
        }
        if ((periodEnd.getValue() - periodBegin.getValue()) <= 0.5) {
            return 0;
        }
        return (periodEnd.getDateTaken().toDate().getTime() - periodBegin.getDateTaken().toDate().getTime())/1000;
    }

    @Override
    public String getSensorUUID() {
        return uuid;
    }

    @Override
    public SensorUnit getUnit() {
        return SensorUnit.GASOLINE_VOLUME;
    }

    @Override
    public SensorType getType() {
        return SensorType.GAS_VOLUME_ODOMETER;
    }
}
