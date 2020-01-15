package com.desi.data.bean;

import com.desi.data.CarSensorRecord;
import com.desi.data.SensorRecord;
import com.desi.data.SensorUnit;
import org.joda.time.LocalDateTime;

import static com.desi.data.SensorUnit.GASOLINE_CONSUMPTION;

public class GasolineConsumptionRecord implements CarSensorRecord {

    private final VehicleImageData imageData;

    private final LocalDateTime dateTaken;

    private final float value;

    private final String uuid;

    public GasolineConsumptionRecord(VehicleImageData imageData, String uuid, float value) {
        this.imageData = imageData;
        this.dateTaken = imageData.getDateTaken();
        this.value = value;
        this.uuid = uuid;
    }

    public GasolineConsumptionRecord(VehicleImageData imageData, String uuid, LocalDateTime dateTaken, float value) {
        this.imageData = imageData;
        this.dateTaken = dateTaken;
        this.value = value;
        this.uuid = uuid;
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
        return GASOLINE_CONSUMPTION;
    }

    @Override
    public String getFileName() {
        if (imageData == null) return null;
        return imageData.getFileName();
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
    public VehicleImageData getImageData() {
        return imageData;
    }

}
