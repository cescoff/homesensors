package com.desi.data.bean;

import com.desi.data.SensorRecord;
import com.desi.data.SensorUnit;
import org.joda.time.LocalDateTime;

import java.util.regex.Matcher;

public class GPSLongitudeSensorRecord implements SensorRecord {

    private final String uuid;

    private final LocalDateTime dateTaken;

    private final float value;

    public GPSLongitudeSensorRecord(String uuid, LocalDateTime dateTaken, String ref, String lat) {
        this.uuid = uuid;
        this.dateTaken = dateTaken;
        final Matcher dmsMatcher = GPSLatitudeSensorRecord.DMS_PATTERN.matcher(lat);
        if (dmsMatcher.find()) {
            int deg = Integer.parseInt(dmsMatcher.group(1));
            double min = Double.parseDouble(dmsMatcher.group(2)) / 60;
            double sec = Double.parseDouble(dmsMatcher.group(3)) / 3600;
            float temp = new Double(deg + min + sec).floatValue();
            if (!"E".equalsIgnoreCase(ref)) {
                temp = -1 * temp;
            }
            this.value = temp;
        } else {
            this.value = 0f;
        }
    }

    public GPSLongitudeSensorRecord(String uuid, LocalDateTime dateTaken, float value) {
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
        return SensorUnit.POSITION;
    }

    public static void main(String[] args) {
        final GPSLatitudeSensorRecord lat = new GPSLatitudeSensorRecord("uuid", LocalDateTime.now(), "N", "48° 49' 32.68\"");
        final GPSLongitudeSensorRecord lon = new GPSLongitudeSensorRecord("uuid", LocalDateTime.now(), "E", "2° 20' 55.93\"");
        System.out.println("Lat/Lon=" + lat.getValue() + "/" + lon.getValue());
    }


}
