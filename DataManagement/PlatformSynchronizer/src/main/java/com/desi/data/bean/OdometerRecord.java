package com.desi.data.bean;

import com.desi.data.CarSensorRecord;
import com.desi.data.SensorRecord;
import com.desi.data.SensorType;
import com.desi.data.SensorUnit;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;

import java.util.List;
import java.util.Vector;

public class OdometerRecord implements CarSensorRecord {

    private final VehicleImageData imageData;

    private final float value;

    private final String uuid;

    private final LocalDateTime dateTaken;

    private final List<String> annotatedTexts = Lists.newArrayList();

    public OdometerRecord(VehicleImageData image, String uuid, float value) {
        this.value = value;
        this.uuid = uuid;
        this.imageData = image;
        this.dateTaken = null;
    }

    public OdometerRecord(VehicleImageData image, String uuid, LocalDateTime dateTaken, float value) {
        this.value = value;
        this.uuid = uuid;
        this.imageData = image;
        this.dateTaken = dateTaken;
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

    public void addTexts(final Iterable<String> texts) {
        Iterables.addAll(this.annotatedTexts, texts);
    }

    public String getFileName() {
        if (imageData == null) return null;
        return imageData.getFileName();
    }

    public List<String> getAnnotatedTexts() {
        return annotatedTexts;
    }

    public boolean isAssociatedGasolineVolume(final GasolineVolumeRecord record) {
        return record.getDateTaken().isAfter(getDateTaken().minusMinutes(10)) && record.getDateTaken().isBefore(getDateTaken().plusMinutes(10));
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

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String uuid;

        private float value;

        private LocalDateTime dateTaken;

        private List<String> textElements = Lists.newArrayList();

        private VehicleImageData vehicleImageData;

        private boolean unreadable = false;

        public Builder withUUID(final String uuid) {
            this.uuid = uuid;
            return this;
        }

        public Builder withValue(final float value) {
            this.value = value;
            return this;
        }

        public Builder withDateTaken(final LocalDateTime dateTaken) {
            this.dateTaken = dateTaken;
            return this;
        }

        public Builder withTextElements(final Iterable<String> textElements) {
            Iterables.addAll(this.textElements, textElements);
            return this;
        }

        public Builder withImage(final AnnotatedImage image) {
            withDateTaken(image.getDateTaken());
            withTextElements(image.getTextElements());
            return this;
        }

        public Builder withUnreadable() {
            this.unreadable = true;
            return this;
        }

        public SensorRecord build() {
            if (unreadable) {
                final UnreadableCarOdometerRecord result = new UnreadableCarOdometerRecord(dateTaken, uuid, null);
                result.addTexts(textElements);
                return result;
            }
            final OdometerRecord result = new OdometerRecord(null, uuid, dateTaken, value);
            result.addTexts(textElements);
            return result;
        }

    }

    @Override
    public SensorType getType() {
        return SensorType.DISTANCE_ODOMETER;
    }
}
