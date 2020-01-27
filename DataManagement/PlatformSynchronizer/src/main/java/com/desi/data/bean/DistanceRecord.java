package com.desi.data.bean;

import com.desi.data.CarSensorRecord;
import com.desi.data.SensorRecord;
import com.desi.data.SensorType;
import com.desi.data.SensorUnit;
import org.joda.time.LocalDateTime;

public class DistanceRecord implements CarSensorRecord {

    private final VehicleImageData imageData;

    private final String uuid;

    private final LocalDateTime dateTaken;

    private final float value;

    public DistanceRecord(VehicleImageData imageData, String uuid, float value) {
        this.imageData = imageData;
        this.uuid = uuid;
        this.dateTaken = imageData.getDateTaken();
        this.value = value;
    }

    public DistanceRecord(VehicleImageData imageData, String uuid, LocalDateTime dateTaken, float value) {
        this.imageData = imageData;
        this.uuid = uuid;
        this.dateTaken = dateTaken;
        this.value = value;
    }

    @Override
    public LocalDateTime getDateTaken() {
        return dateTaken;
    }

    @Override
    public float getValue() {
        return value;
    }

    @Override
    public String getSensorUUID() {
        return uuid;
    }

    @Override
    public SensorUnit getUnit() {
        return SensorUnit.KILOMETER;
    }

    @Override
    public float getLatitude() {
        if (imageData == null) return 0;
        return imageData.getLatitude();
    }

    @Override
    public float getLongitude() {
        if (imageData == null) return 0;
        return imageData.getLongitude();
    }

    @Override
    public float getAltitude() {
        if (imageData == null) return 0;
        return imageData.getAltitude();
    }

    @Override
    public String getFileName() {
        return null;
    }

    @Override
    public VehicleImageData getImageData() {
        return imageData;
    }

    @Override
    public SensorType getType() {
        return SensorType.DISTANCE;
    }
}
