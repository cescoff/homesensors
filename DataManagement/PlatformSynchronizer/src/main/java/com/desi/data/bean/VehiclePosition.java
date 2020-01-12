package com.desi.data.bean;

import com.desi.data.SensorRecord;
import com.desi.data.SensorUnit;
import org.joda.time.LocalDateTime;

public class VehiclePosition implements SensorRecord, Exportable  {

    private final String uuid;

    private final LocalDateTime dateTime;

    private final float latitude;

    private final float longitude;

    private final float speed;

    public VehiclePosition(String uuid, LocalDateTime dateTime, float latitude, float longitude, float speed) {
        this.uuid = uuid;
        this.dateTime = dateTime;
        this.latitude = latitude;
        this.longitude = longitude;
        this.speed = speed;
    }

    public LocalDateTime getDateTime() {
        return dateTime;
    }

    public float getLatitude() {
        return latitude;
    }

    public float getLongitude() {
        return longitude;
    }

    public float getSpeed() {
        return speed;
    }

    @Override
    public LocalDateTime getDateTaken() {
        return dateTime;
    }

    @Override
    public float getValue() {
        return latitude;
    }

    @Override
    public String getSensorUUID() {
        return uuid;
    }

    @Override
    public SensorUnit getUnit() {
        return SensorUnit.POSITION;
    }

    @Override
    public String toCSVLine() {
        return "";
    }

    public String toSQL(final String dataSet, final String tableName) {
        final StringBuilder result = new StringBuilder("INSERT INTO ").append(dataSet).append(".").append(tableName).append(" (uuid, position, dateTime) VALUES (\"");
        result.append(uuid).append("\", ST_GEOGPOINT(").append(longitude).append(", ").append(latitude).append("), \"").append(dateTime.toString()).append("\");\n");
        return result.toString();
    }

}
