package com.desi.data.bean;

import com.desi.data.SensorRecord;
import com.desi.data.SensorUnit;
import org.joda.time.LocalDateTime;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GPSLatitudeSensorRecord implements SensorRecord {

    public static final Pattern DMS_PATTERN = Pattern.compile("([0-9]+)[\\s]*°[\\s]*([0-9]+)[\\s]*'[\\s]*([0-9\\.]+)[\\s]*\"");

    private final String uuid;

    private final LocalDateTime dateTaken;

    private final float value;

    public GPSLatitudeSensorRecord(String uuid, LocalDateTime dateTaken, String ref, String lat) {
        this.uuid = uuid;
        this.dateTaken = dateTaken;
        final Matcher dmsMatcher = DMS_PATTERN.matcher(lat);
        if (dmsMatcher.find()) {
            int deg = Integer.parseInt(dmsMatcher.group(1));
            double min = Double.parseDouble(dmsMatcher.group(2)) / 60;
            double sec = Double.parseDouble(dmsMatcher.group(3)) / 3600;
            float temp = new Double(deg + min + sec).floatValue();
            if (!"N".equalsIgnoreCase(ref)) {
                temp = -1 * temp;
            }
            this.value = temp;
        } else {
            this.value = 0f;
        }
    }

    public GPSLatitudeSensorRecord(String uuid, LocalDateTime dateTaken, float value) {
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
}
