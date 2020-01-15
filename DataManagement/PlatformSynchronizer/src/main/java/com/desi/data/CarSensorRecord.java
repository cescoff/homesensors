package com.desi.data;

import com.desi.data.bean.VehicleImageData;

public interface CarSensorRecord extends SensorRecord {

    public float getLatitude();

    public float getLongitude();

    public float getAltitude();

    public String getFileName();

    public VehicleImageData getImageData();

}
