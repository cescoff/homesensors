package com.desi.data.bean;

import com.desi.data.CarSensorRecord;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;

public interface VehicleImageData extends CarSensorRecord {

    public String getUUID();

    public float getOdometerValue();

    public float getVolume();

    public float getPrice();

    public float getPricePerLitre();

    public boolean hasVolume();

    public boolean hasPrice();

    public boolean hasPricePerLitre();

    public boolean hasOdometerValue();

    public String getFileName();

    public LocalDateTime getDateTaken();

    public float getLatitude();

    public float getLongitude();

    public float getAltitude();

}
