package com.desi.data.bean;

import com.desi.data.CarSensorRecord;
import com.desi.data.SensorRecord;
import com.desi.data.SensorType;
import com.desi.data.SensorUnit;
import org.joda.time.LocalDateTime;

public class VehiclePosition implements CarSensorRecord, Exportable  {

    final VehicleImageData imageData;

    private final String uuid;

    private final LocalDateTime dateTime;

    private final float latitude;

    private final float longitude;

    private final float altitude;

    private final float speed;

    private final String fileName;

    public VehiclePosition(VehicleImageData imageData, String uuid, String fileName, LocalDateTime dateTime, float latitude, float longitude, float altitude, float speed) {
        this.imageData = imageData;
        this.uuid = uuid;
        this.fileName = fileName;
        this.dateTime = dateTime;
        this.latitude = latitude;
        this.longitude = longitude;
        this.altitude = altitude;
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

    @Override
    public float getAltitude() {
        return 0;
    }

    @Override
    public VehicleImageData getImageData() {
        return imageData;
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

    public String getFileName() {
        return fileName;
    }

    @Override
    public SensorType getType() {
        return SensorType.POSITION;
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

    @Override
    public String toString() {
        return "VehiclePosition{" +
                "uuid='" + uuid + '\'' +
                ", dateTime=" + dateTime +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", speed=" + speed +
                ", fileName='" + fileName + '\'' +
                '}';
    }
}
