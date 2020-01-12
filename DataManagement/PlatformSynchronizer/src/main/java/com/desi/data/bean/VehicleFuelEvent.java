package com.desi.data.bean;


import com.desi.data.SensorRecord;
import com.desi.data.SensorUnit;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class VehicleFuelEvent implements SensorRecord, Exportable {

    private DateTimeFormatter DATETIME_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm");

    private final String uuid;

    private final LocalDateTime dateTime;

    private final float odometerValue;

    private final float fuelVolume;

    private final float fuelPrice;

    private final float distance;

    private final float consumption;

    private final float latitude;

    private final float longitude;

    public VehicleFuelEvent(String uuid, LocalDateTime dateTime, float odometerValue, float fuelVolume, float fuelPrice, float distance, float consumption, float latitude, float longitude) {
        this.uuid = uuid;
        this.dateTime = dateTime;
        this.odometerValue = odometerValue;
        this.fuelVolume = fuelVolume;
        this.fuelPrice = fuelPrice;
        this.distance = distance;
        this.consumption = consumption;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public String getUuid() {
        return uuid;
    }

    public LocalDateTime getDateTime() {
        return dateTime;
    }

    public float getOdometerValue() {
        return odometerValue;
    }

    public float getFuelVolume() {
        return fuelVolume;
    }

    public float getFuelPrice() {
        return fuelPrice;
    }

    public float getDistance() {
        return distance;
    }

    public float getConsumption() {
        return consumption;
    }

    public float getLatitude() {
        return latitude;
    }

    public float getLongitude() {
        return longitude;
    }

    @Override
    public LocalDateTime getDateTaken() {
        return dateTime;
    }

    @Override
    public float getValue() {
        return getConsumption();
    }

    @Override
    public String getSensorUUID() {
        return uuid;
    }

    @Override
    public SensorUnit getUnit() {
        return SensorUnit.GASOLINE_CONSUMPTION;
    }

    public String toString() {
        final StringBuilder result = new StringBuilder();
        result.append(DATETIME_FORMAT.print(dateTime)).
                append(" : uuid='").append(uuid).
                append("', odometer=").append(odometerValue).append(" km").
                append(", fuel=").append(fuelVolume).append(" L").
                append(", price=").append(fuelPrice).append(" €").
                append(", distance=").append(distance).append(" km").
                append(", consumption=").append(consumption).append(" L/100km");
        return result.toString();
    }

    @Override
    public String toCSVLine() {
        return new StringBuilder("\"").append(uuid).append("\",\"").append(DATETIME_FORMAT.print(dateTime)).append("\",").
                append(odometerValue).append(",").
                append(fuelVolume).append(",").
                append(distance).append(",").
                append(fuelPrice).append(",").
                append(consumption).toString();
    }

    public String toSQL(final String schema, final String tableName) {
        // INSERT INTO CarSensors.VehiculeFuelEvent (uuid, dateTime, odometerValue, fuelVolume, fuelPrice, distance, consumption) VALUES ("c1e22264-a62e-4662-909f-7c31798d231e", "2017-08-05T11:56:00", 842692.0, 15.58, 1.6501925, 238.0, 6.5462184)
        final StringBuilder result = new StringBuilder("INSERT INTO ");
        result.append(schema).append(".").append(tableName);
        result.append(" (uuid, dateTime, odometerValue, fuelVolume, fuelPrice, distance, consumption) VALUES (\"");
        result.append(uuid).append("\",").
                append("\"").append(dateTime.toString()).append("\",").
                append(odometerValue).append(", ").
                append(fuelVolume).append(", ").
                append(fuelPrice).append(", ").
                append(distance).append(", ").
                append(consumption).append(");\n");
        return result.toString();
    }

}
