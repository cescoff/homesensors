package com.desi.data.utils;

import com.desi.data.ImageAnnotator;
import com.desi.data.SensorRecord;
import com.desi.data.SensorUnit;
import com.desi.data.bean.*;
import com.desi.data.config.CarConfiguration;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.PropertyConfigurator;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class ImageAnalyzer {

    private static final String UNKNOWN_ODOMETER_UUID = "52f88258-fbd6-419f-9fc6-286724d9fbc6";
    private static final String UNKNOWN_GASOLINE_METER_UUID = "54dd4a04-26d8-447b-a595-6665b014d929";

    private static final String GASOLINE_PRICE_UUID = "9762b419-c5bd-4285-bce9-496485e3b710";

    private static final Pattern CAR_ODOMETER_PARSER = Pattern.compile("([0-9]{6})");
    private static final Pattern GASOLINE_VOLUME_PARSER = Pattern.compile("([0-9]{1,2}[,\\.]+[0-9]{0,2})");

    private static final Pattern NUMBER_TEXT_PATTERN = Pattern.compile("([\\-]*[0-9]+[\\.\\,]{0,1}[0-9]*)");

    private static final String[] PEUGEOT_305_ODOMETER_MARKERS = {"PJ74", "JAEGER"};
    private static final String[] GASOLINE_METER_MARKERS = {"LITRE", "VOLUME"};


    private static Logger logger = LoggerFactory.getLogger(ImageAnalyzer.class);

    private ImageAnalyzer() {}

    private static boolean containsAnyIgnoreCase(final String text, final String[] values) {
        for (final String value : values) {
            if (StringUtils.containsIgnoreCase(text, value)) {
                return true;
            }
        }
        return false;
    }

    public static Iterable<SensorRecord> analyze(final AnnotatedImage image) {
        final ImmutableList.Builder<SensorRecord> result = ImmutableList.builder();
        boolean carOdometerImage = false;
        boolean gasolineVolumeImage = false;
        for (final String text : image.getTextElements()) {
            if (containsAnyIgnoreCase(text, PEUGEOT_305_ODOMETER_MARKERS)) {
                carOdometerImage = true;
            }
            if (containsAnyIgnoreCase(text, GASOLINE_METER_MARKERS)) {
                gasolineVolumeImage = true;
            }
        }
        if (carOdometerImage) {
            logger.info("Car odometer found");
            float odometerValue = 0;
            for (final String text : image.getTextElements()) {
                final Matcher matcher = CAR_ODOMETER_PARSER.matcher(text);
                if (matcher.find()) {
                    float value = new Float(Integer.parseInt(StringUtils.trim(matcher.group(1))));
                    if (odometerValue <= 0) {
                        logger.info("Found car odometer value : " + matcher.group(1));
                        result.add(OdometerRecord.builder().withImage(image).withUUID(CarConfigurationHelper.getPeugeot305().getOdometerUUID()).withValue(value).build());
                        odometerValue = value;
                    }
                }
            }
            if (odometerValue <= 0) {
                result.add(OdometerRecord.builder().withImage(image).withUUID(CarConfigurationHelper.getPeugeot305().getOdometerUUID()).withUnreadable().build());
            }
            if (StringUtils.isNotEmpty(image.getLatitude()) && StringUtils.isNotEmpty(image.getLongitude())) {
                result.add(new GPSLatitudeSensorRecord(CarConfigurationHelper.getPeugeot305().getGPSUUID(), image.getDateTaken(), image.getLatitudeRef(), image.getLatitude()));
                result.add(new GPSLongitudeSensorRecord(CarConfigurationHelper.getPeugeot305().getGPSUUID(), image.getDateTaken(), image.getLongitudeRef(), image.getLongitude()));
            }
        }
        if (gasolineVolumeImage) {
            logger.info("Gasoline volume found");
            float volume = 0;
            float fullPrice = 0;
            float gasolinePrice = 0;
            SensorUnit currency = SensorUnit.EURO;
            for (int position = 0; position < Iterables.size(image.getTextElements()); position++) {
                final String text = Iterables.get(image.getTextElements(), position);
                if (SensorUnit.getCurrency(text).isPresent()) {
                    currency = SensorUnit.getCurrency(text).get();
                }
                if (!StringUtils.contains(text, "\n")) {
                    if ((isNumber(text) && StringUtils.containsIgnoreCase(text, "Litre")) || (isNumber(text) && hasNextToken(image.getTextElements(), "Litre", position))) {
                        final Matcher matcher = GASOLINE_VOLUME_PARSER.matcher(Iterables.get(image.getTextElements(), position));

                        if (matcher.find()) {
                            final String gasolineVolumeValue = StringUtils.replace(matcher.group(1), ",", ".");
                            if (volume <= 0) {
                                volume = Float.parseFloat(gasolineVolumeValue);
                                logger.info("Found gasoline volume value : '" + gasolineVolumeValue);
                                result.add(GasolineVolumeRecord.builder().withImage(image).withUUID(UNKNOWN_GASOLINE_METER_UUID).withValue(volume).build());
                            } else {
                                float value = Float.parseFloat(gasolineVolumeValue);
                                if (value > volume) {
                                    gasolinePrice = value / volume;
                                }
                            }
                        } else {
                            final Matcher fakeIntMatcher = Pattern.compile("([0-9]{4})").matcher(text);
                            if (fakeIntMatcher.find()) {
                                final String gasolineVolumeValue = StringUtils.substring(fakeIntMatcher.group(1), 0, 2) + "." + StringUtils.substring(fakeIntMatcher.group(1), 2);
                                if (volume <= 0) {
                                    volume = Float.parseFloat(gasolineVolumeValue);
                                    logger.info("Found gasoline volume value : " + gasolineVolumeValue);
                                    result.add(GasolineVolumeRecord.builder().withImage(image).withUUID(UNKNOWN_GASOLINE_METER_UUID).withValue(volume).build());
                                } else {
                                    float value = Float.parseFloat(gasolineVolumeValue);
                                    if (value > volume) {
                                        gasolinePrice = value / volume;
                                    } else if (isValidGasolinePrice(value)) {
                                        gasolinePrice = value;
                                    }
                                }
                            }
                        }
                    } else if (isNumber(text)) {
                        final Matcher numberMatcher = NUMBER_TEXT_PATTERN.matcher(text);
                        if (numberMatcher.find()) {
                            float value = Float.parseFloat(StringUtils.replace(numberMatcher.group(1), ",", "."));
                            if (isValidGasolinePrice(value)) gasolinePrice = value;
                            else if (value < 150) fullPrice = value;
                        }
                    } else {
                        logger.info("Nothing can be done there");
                    }
                } else {
                    for (final String line : StringUtils.split(text, "\n")) {
                        final Matcher matcher = GASOLINE_VOLUME_PARSER.matcher(line);
                        if (matcher.find() && StringUtils.containsIgnoreCase(line, "Litre") && volume <= 0) {
                            final String gasolineVolumeValue = StringUtils.replace(matcher.group(1), ",", ".");
                            volume = Float.parseFloat(gasolineVolumeValue);
                            logger.info("Found gasoline volume value : '" + gasolineVolumeValue);
                            result.add(GasolineVolumeRecord.builder().withImage(image).withUUID(UNKNOWN_GASOLINE_METER_UUID).withValue(volume).build());
                        }
                    }
                }
            }
            if (volume <= 0) {
                List<Float> values = Lists.newArrayList();
                for (final String text : image.getTextElements()) {
                    if (!StringUtils.contains(text, "\n")) {
                        final Matcher volumeMatcher = GASOLINE_VOLUME_PARSER.matcher(text);
                        if (volumeMatcher.find()) {
                            float value = Float.parseFloat(StringUtils.replace(volumeMatcher.group(1), ",", "."));
                            if (value > 0 && value < 200) {
                                values.add(value);
                            }
                        } else {
                            final Matcher fakeIntMatcher = Pattern.compile("([0-9]+)").matcher(text);
                            // Fake int is when comma or dot is not read by OCR
                            if (fakeIntMatcher.find() && fakeIntMatcher.group(1).length() == 4) {
                                final String gasolineVolumeValue = StringUtils.substring(fakeIntMatcher.group(1), 0, 2) + "." + StringUtils.substring(fakeIntMatcher.group(1), 2);
                                float value = Float.parseFloat(gasolineVolumeValue);
                                if (value > 0 && value < 200) {
                                    values.add(value);
                                }
                            }
                        }
                    }
                }
                values = Ordering.natural().reverse().sortedCopy(values);
                if (values.size() > 1) {
                    fullPrice = values.get(0);
                    volume = values.get(1);
                    result.add(GasolineVolumeRecord.builder().withImage(image).withUUID(UNKNOWN_GASOLINE_METER_UUID).withValue(volume).build());
                    if (fullPrice > volume) {
                        gasolinePrice = fullPrice / volume;
                        if (isValidGasolinePrice(gasolinePrice)) {
                            result.add(new PriceRecord(image.getDateTaken(), GASOLINE_PRICE_UUID, gasolinePrice, currency));
                        }
                    }
                } else if (values.size() == 1) {
                    result.add(GasolineVolumeRecord.builder().withImage(image).withUUID(UNKNOWN_GASOLINE_METER_UUID).withValue(values.get(0)).build());
                } else {
                    logger.info("Unreadable gasoline volume value : '" + Joiner.on("', '").join(image.getTextElements()) + "'");
                    result.add(GasolineVolumeRecord.builder().withImage(image).withUUID(UNKNOWN_GASOLINE_METER_UUID).withUnreadable("Values are unclear too manu of them found (" + Joiner.on(", ").join(values) + ")").build());
                }
            } else if (isValidGasolinePrice(gasolinePrice)) {
                result.add(new PriceRecord(image.getDateTaken(), GASOLINE_PRICE_UUID, gasolinePrice, currency));
            } else if (fullPrice > volume) {
                gasolinePrice = fullPrice / volume;
                if (isValidGasolinePrice(gasolinePrice)) {
                    result.add(new PriceRecord(image.getDateTaken(), GASOLINE_PRICE_UUID, gasolinePrice, currency));
                }
            }
        }
        return result.build();
    }

    private static boolean isValidGasolinePrice(float value) {
        return value > 1.4 && value < 1.8;
    }

    private static Iterable<SensorRecord> aggregateValues(final Iterable<SensorRecord> records) {
        List<OdometerRecord> odometerValues = Lists.newArrayList();
        List<GasolineVolumeRecord> gasolineVolumeRecords = Lists.newArrayList();
        List<SensorRecord> validVolumeRecords = Lists.newArrayList();
        List<SensorRecord> validGasolinePrices = Lists.newArrayList();
        final List<SensorRecord> otherValues = Lists.newArrayList();
        final List<SensorRecord> errors = Lists.newArrayList();

        for (final SensorRecord sensorRecord : records) {
            if (sensorRecord instanceof OdometerRecord) {
                odometerValues.add((OdometerRecord) sensorRecord);
            } else if (sensorRecord instanceof GasolineVolumeRecord) {
                gasolineVolumeRecords.add((GasolineVolumeRecord) sensorRecord);
/*            } else if (sensorRecord instanceof  PriceRecord){*/
            } else {
                otherValues.add(sensorRecord);
            }
        }

        odometerValues = Ordering.natural().onResultOf(SENSOR_RECORD_SORTER).sortedCopy(odometerValues);
        gasolineVolumeRecords = Ordering.natural().onResultOf(SENSOR_RECORD_SORTER).sortedCopy(gasolineVolumeRecords);

        for (int odometerPosition = 0; odometerPosition < odometerValues.size(); odometerPosition++) {
            OdometerRecord odometerRecord = odometerValues.get(odometerPosition);
            GasolineVolumeRecord gasolineVolumeRecord = getAssociatedGasolineVolume(gasolineVolumeRecords, odometerRecord);
            if (gasolineVolumeRecord != null) {
                CarConfiguration carConfiguration = CarConfigurationHelper.resolveUUID(odometerRecord.getSensorUUID());
                if (carConfiguration.isValidGasolineVolume(gasolineVolumeRecord.getValue())) {

                    final OdometerRecord previousRecord = getPreviousOdometerValue(odometerValues, gasolineVolumeRecords, odometerPosition, odometerRecord);

                    if (previousRecord != null) {
                        float distanceBetween2Refuel = odometerRecord.getValue() - previousRecord.getValue();
                        if (distanceBetween2Refuel > 0 && carConfiguration.isValidDistanceBetween2ReFuel(distanceBetween2Refuel)) {
                            GasolineConsumptionRecord gasolineConsumptionRecord = new GasolineConsumptionRecord(odometerRecord.getDateTaken(), (100*gasolineVolumeRecord.getValue()) / distanceBetween2Refuel, carConfiguration.getGasolineAvgConsumptionUUID());
                            if (carConfiguration.isValidGasolineConsumption(gasolineConsumptionRecord.getValue())) {
                                otherValues.add(gasolineConsumptionRecord);
                                validVolumeRecords.add(GasolineVolumeRecord.builder().withPreviousRecord(gasolineVolumeRecord).withUUID(carConfiguration.getGasolineMeterUUID()).build());
                            } else {
                                errors.add(GasolineVolumeRecord.builder().withPreviousRecord(gasolineVolumeRecord).withUUID(carConfiguration.getGasolineMeterUUID()).withUnreadable(
                                        "Gasoline consumption is not acceptable for that vehicle "
                                                + gasolineConsumptionRecord.getValue()
                                                + "L/100, gasoline volume is "
                                                + gasolineVolumeRecord.getValue()
                                                + " and odometer value "
                                                + odometerRecord.getValue()
                                                + "km and distance "
                                                + distanceBetween2Refuel).build());
                            }
                        }
                    }

                } else {
                    errors.add(GasolineVolumeRecord.builder().withPreviousRecord(gasolineVolumeRecord).withUUID(carConfiguration.getGasolineMeterUUID()).withUnreadable("Gasoline volume is not acceptable for that vehicle " + gasolineVolumeRecord.getValue()).build());
                }
            }
        }
        return ImmutableList.copyOf(
                Ordering.natural().onResultOf(SENSOR_RECORD_SORTER).sortedCopy(
                        ImmutableList.<SensorRecord>builder().addAll(odometerValues).addAll(validVolumeRecords).addAll(otherValues).addAll(errors).build()));
    }

    private static GasolineVolumeRecord getAssociatedGasolineVolume(final List<GasolineVolumeRecord> records, final OdometerRecord odometerRecord) {
        for (int index = 0; index < records.size(); index++) {
            if (odometerRecord.isAssociatedGasolineVolume(records.get(index))) {
                return records.get(index);
            }
        }
        return null;
    }

    private static OdometerRecord getPreviousOdometerValue(final List<OdometerRecord> records, final List<GasolineVolumeRecord> gasolineVolumeRecords, final int position, final OdometerRecord odometerRecord) {
        for (int index = 1; index <= position; index++) {
            OdometerRecord candidate = records.get(position - index);
            if (candidate.getSensorUUID().equals(odometerRecord.getSensorUUID()) && candidate.getDateTaken().isBefore(odometerRecord.getDateTaken().minusMinutes(10))) {
                if (getAssociatedGasolineVolume(gasolineVolumeRecords, candidate) != null) {
                    return candidate;
                }
            }
        }
        return null;
    }

    private static Function<SensorRecord, Date> SENSOR_RECORD_SORTER = new Function<SensorRecord, Date>() {
        @Nullable
        @Override
        public Date apply(@Nullable SensorRecord sensorRecord) {
            return sensorRecord.getDateTaken().toDate();
        }
    };

    private static boolean hasNextToken(final Iterable<String> textElements, final String value, final int from) {
        int position = from + 1;
        while (position < Iterables.size(textElements)) {
            if (!isNumber(Iterables.get(textElements, position))) {
                return StringUtils.containsIgnoreCase(Iterables.get(textElements, position), value);
            }
            position++;
        }
        return false;
    }

    private static boolean isNumber(final String text) {
        return NUMBER_TEXT_PATTERN.matcher(text).find();
    }

    public static Iterable<SensorRecord> analyseFlow(final Iterable<AnnotatedImage> images) {
        ImmutableList.Builder<SensorRecord> result = ImmutableList.builder();
        for (final AnnotatedImage annotatedImage : images) {
            result.addAll(analyze(annotatedImage));
        }
        return aggregateValues(result.build());
    }

    public static void main(String[] args) throws JAXBException, IOException {
        final Properties logConfig = new Properties();

        logConfig.setProperty("log4j.rootLogger", "INFO, Appender1,Appender2");
        logConfig.setProperty("log4j.appender.Appender1", "org.apache.log4j.ConsoleAppender");
        logConfig.setProperty("log4j.appender.Appender1.layout", "org.apache.log4j.PatternLayout");
        logConfig.setProperty("log4j.appender.Appender1.layout.ConversionPattern", "%-7p %d [%t] %c %x - %m%n");
        logConfig.setProperty("log4j.appender.Appender2", "org.apache.log4j.FileAppender");
        logConfig.setProperty("log4j.appender.Appender2.File", "logs/synchronizer.log");
        logConfig.setProperty("log4j.appender.Appender2.layout", "org.apache.log4j.PatternLayout");
        logConfig.setProperty("log4j.appender.Appender2.layout.ConversionPattern", "%-7p %d [%t] %c %x - %m%n");

        PropertyConfigurator.configure(logConfig);

        final FileOutputStream errorsOut = new FileOutputStream(new File("errors.txt"));

        final ImageAnnotator.AnnotatedImageBatch batch = JAXBUtils.unmarshal(ImageAnnotator.AnnotatedImageBatch.class, new File("/Users/corentin/Documents/Developpement/image-annotations.xml"));
        for (final SensorRecord sensorRecord : ImageAnalyzer.analyseFlow(Iterables.filter(batch.getAnnotatedImages(), DATE_FILTER))) {
            logger.info(sensorRecord.getDateTaken() + " : Sensor[" + sensorRecord.getSensorUUID() + "] : " + sensorRecord.getValue() + sensorRecord.getUnit().getDisplay());
            if (sensorRecord.getUnit() == SensorUnit.UNREADABLE_GASOLINE_VOLUME || sensorRecord.getUnit() == SensorUnit.UNREADABLE_ODOMETER) {
                final StringBuilder errorMessage = new StringBuilder();
                errorMessage.append("DateTaken: ").append(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm").print(sensorRecord.getDateTaken())).append("\n");
                if (sensorRecord instanceof UnreadableGasolineVolumeRecord) {
                    UnreadableGasolineVolumeRecord unreadableGasolineVolumeRecord = (UnreadableGasolineVolumeRecord) sensorRecord;
                    errorMessage.append("FileName: ").append(unreadableGasolineVolumeRecord.getFileName()).append("\n");
                    errorMessage.append("Reason: ").append(unreadableGasolineVolumeRecord.getReason()).append("\n");
                    errorMessage.append("Annotations:\n");
                    for (final String annotation : unreadableGasolineVolumeRecord.getAnnotatedTexts()) {
                        errorMessage.append("  - '").append(Joiner.on("', '").join(Lists.newArrayList(StringUtils.split(annotation, "\n")))).append("'\n");
                    }
                    errorMessage.append("_______________________________________________________\n");
                }
                errorsOut.write(errorMessage.toString().getBytes());
            }
        }
        errorsOut.close();
    }

    private static final Predicate<AnnotatedImage> DATE_FILTER = new Predicate<AnnotatedImage>() {
        @Override
        public boolean apply(@Nullable AnnotatedImage annotatedImage) {
            return annotatedImage.getDateTaken().isAfter(new LocalDateTime("2018-9-25T00:00:00"));
        }
    };

}
