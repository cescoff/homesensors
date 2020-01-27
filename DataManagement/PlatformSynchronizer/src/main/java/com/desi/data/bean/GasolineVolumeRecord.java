package com.desi.data.bean;

import com.desi.data.SensorRecord;
import com.desi.data.SensorType;
import com.desi.data.SensorUnit;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.joda.time.LocalDateTime;

import java.util.List;

public class GasolineVolumeRecord implements SensorRecord {

    private final LocalDateTime dateTaken;

    private final float value;

    private final String uuid;

    private final String fileName;

    private final List<String> annotatedTexts = Lists.newArrayList();

    public GasolineVolumeRecord(LocalDateTime dateTaken, float value, String uuid, String fileName) {
        this.dateTaken = dateTaken;
        this.value = value;
        this.uuid = uuid;
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
        return SensorUnit.GASOLINE_VOLUME;
    }

    public void addTexts(final Iterable<String> texts) {
        Iterables.addAll(this.annotatedTexts, texts);
    }

    public List<String> getAnnotatedTexts() {
        return annotatedTexts;
    }

    public String getFileName() {
        return fileName;
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

        private String unreadableReason = null;

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

        public Builder withPreviousRecord(final GasolineVolumeRecord record) {
            withFileName(record.fileName);
            withDateTaken(record.getDateTaken());
            withTextElements(record.annotatedTexts);
            withUUID(record.uuid);
            withValue(record.value);
            return this;
        }

        public Builder withUnreadable(final String reason) {
            this.unreadable = true;
            this.unreadableReason = reason;
            return this;
        }

        public SensorRecord build() {
            if (unreadable) {
                final UnreadableGasolineVolumeRecord result = new UnreadableGasolineVolumeRecord(dateTaken, uuid, fileName, unreadableReason);
                result.addTexts(textElements);
                return result;
            }
            final GasolineVolumeRecord result = new GasolineVolumeRecord(dateTaken, value, uuid, fileName);
            result.addTexts(textElements);
            return result;
        }

    }

    @Override
    public SensorType getType() {
        return SensorType.GASOLINE_VOLUME_ODOMETER;
    }
}
