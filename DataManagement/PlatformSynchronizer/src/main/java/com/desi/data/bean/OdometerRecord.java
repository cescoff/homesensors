package com.desi.data.bean;

import com.desi.data.SensorRecord;
import com.desi.data.SensorUnit;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;

import java.util.List;

public class OdometerRecord implements SensorRecord {

    private final float value;

    private final String uuid;

    private final LocalDateTime dateTaken;

    private final String fileName;

    private final List<String> annotatedTexts = Lists.newArrayList();

    public OdometerRecord(float value, String uuid, LocalDateTime dateTaken, String fileName) {
        this.value = value;
        this.uuid = uuid;
        this.dateTaken = dateTaken;
        this.fileName = fileName;
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
        return fileName;
    }

    public List<String> getAnnotatedTexts() {
        return annotatedTexts;
    }

    public boolean isAssociatedGasolineVolume(final GasolineVolumeRecord record) {
        return record.getDateTaken().isAfter(getDateTaken().minusMinutes(10)) && record.getDateTaken().isBefore(getDateTaken().plusMinutes(10));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String fileName;

        private String uuid;

        private float value;

        private LocalDateTime dateTaken;

        private List<String> textElements = Lists.newArrayList();

        private boolean unreadable = false;

        public Builder withFileName(final String fileName) {
            this.fileName = fileName;
            return this;
        }

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
            withFileName(image.getFileName());
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
                final UnreadableCarOdometerRecord result = new UnreadableCarOdometerRecord(dateTaken, uuid, fileName);
                result.addTexts(textElements);
                return result;
            }
            final OdometerRecord result = new OdometerRecord(value, uuid, dateTaken, fileName);
            result.addTexts(textElements);
            return result;
        }

    }

}
