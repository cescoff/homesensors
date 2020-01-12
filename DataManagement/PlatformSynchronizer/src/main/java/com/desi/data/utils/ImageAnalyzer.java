package com.desi.data.utils;

import com.desi.data.ImageAnnotator;
import com.desi.data.SensorRecord;
import com.desi.data.SensorUnit;
import com.desi.data.bean.*;
import com.desi.data.config.CarConfiguration;
import com.desi.data.config.FuelOCRParser;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public abstract class ImageAnalyzer {

    private static final String UNKNOWN_ODOMETER_UUID = "52f88258-fbd6-419f-9fc6-286724d9fbc6";
    private static final String UNKNOWN_GASOLINE_METER_UUID = "54dd4a04-26d8-447b-a595-6665b014d929";

    private static final String GASOLINE_PRICE_UUID = "9762b419-c5bd-4285-bce9-496485e3b710";

    private static final Pattern GASOLINE_VOLUME_PARSER = Pattern.compile("([0-9]{1,2}[,\\.]+[0-9]{0,2})");

    private static final Pattern NUMBER_TEXT_PATTERN = Pattern.compile("([\\-]*[0-9]+[\\.\\,]{0,1}[0-9]*)");

    private static final String[] GASOLINE_METER_MARKERS = {"LITRE", "VOLUME"};


    private static Logger logger = LoggerFactory.getLogger(ImageAnalyzer.class);

    private ImageAnalyzer() {}

    public static Iterable<SensorRecord> getFuelStatistics(final Iterable<AnnotatedImage> images) {
        final List<SensorRecord> result = Lists.newArrayList();
        for (final CarConfiguration carConfiguration : CarConfigurationHelper.all()) {
            final FuelOCRParser parser = new FuelOCRParser(carConfiguration);
            final Iterable<VehicleImageData> fuelStatistics = parser.analyzeImages(images);

            final List<VehicleImageData> odometerValues = Ordering.natural().reverse().onResultOf(new Function<VehicleImageData, Date>() {
                @Nullable
                @Override
                public Date apply(@Nullable VehicleImageData fuelStatistics) {
                    return fuelStatistics.getDateTaken().toDate();
                }
            }).sortedCopy(Iterables.filter(fuelStatistics, new Predicate<VehicleImageData>() {
                @Override
                public boolean apply(@Nullable VehicleImageData fuelStatistics) {
                    return fuelStatistics.hasOdometerValue();
                }
            }));

            final List<VehicleImageData> fuelValues = Ordering.natural().reverse().onResultOf(new Function<VehicleImageData, Date>() {
                @Nullable
                @Override
                public Date apply(@Nullable VehicleImageData fuelStatistics) {
                    return fuelStatistics.getDateTaken().toDate();
                }
            }).sortedCopy(Iterables.filter(fuelStatistics, new Predicate<VehicleImageData>() {
                @Override
                public boolean apply(@Nullable VehicleImageData fuelStatistics) {
                    return fuelStatistics.hasVolume();
                }
            }));

            final List<VehicleImageData> otherValues = Ordering.natural().reverse().onResultOf(new Function<VehicleImageData, Date>() {
                @Nullable
                @Override
                public Date apply(@Nullable VehicleImageData fuelStatistics) {
                    return fuelStatistics.getDateTaken().toDate();
                }
            }).sortedCopy(Iterables.filter(fuelStatistics, new Predicate<VehicleImageData>() {
                @Override
                public boolean apply(@Nullable VehicleImageData fuelStatistics) {
                    return !fuelStatistics.hasVolume() && !fuelStatistics.hasOdometerValue()
                            && !fuelStatistics.hasPrice() && !fuelStatistics.hasPricePerLitre();
                }
            }));

            final Map<VehicleImageData, List<VehicleImageData>> fuelEvents = Maps.newHashMap();
            VehicleImageData currentOdometerValue = null;
            List<VehicleImageData> currentBuffer = Lists.newArrayList();
            for (final VehicleImageData fuelValue : fuelValues) {
                final Iterable<VehicleImageData> odometers = Iterables.filter(odometerValues, ConcomittentDateFilter(fuelValue));
                if (!Iterables.isEmpty(odometers)) {
                    if (currentOdometerValue != null) {
                        fuelEvents.put(currentOdometerValue, currentBuffer);
                        currentBuffer = Lists.newArrayList();
                    }
                    currentOdometerValue = Iterables.getFirst(odometers, null);
                }
                currentBuffer.add(fuelValue);
            }
            fuelEvents.put(currentOdometerValue, currentBuffer);

            for (int index = 0; index < odometerValues.size(); index++) {
                final VehicleImageData odometerValue = odometerValues.get(index);
                result.add(new OdometerRecord(odometerValue.getOdometerValue(), carConfiguration.getOdometerUUID(), odometerValue.getDateTaken(), odometerValue.getFileName()));
                if (fuelEvents.containsKey(odometerValue) && index < (odometerValues.size() - 1)) {
                    final VehicleImageData previousValue = odometerValues.get(index + 1);;
                    final float distance = odometerValue.getOdometerValue() - previousValue.getOdometerValue();

                    if (distance > 0) {
                        float fullVolume = 0;
                        float priceSum = 0;
                        float volumeSum = 0;

                        for (final VehicleImageData fuelEvent : fuelEvents.get(odometerValue)) {
                            if (fuelEvent.hasPricePerLitre()) {
                                result.add(new PriceRecord(odometerValue.getDateTaken(), carConfiguration.getDistanceUUID(), fuelEvent.getPricePerLitre(), SensorUnit.EURO));
                            }

                            fullVolume += fuelEvent.getVolume();
                            if (fuelEvent.getPricePerLitre() > 0) {
                                priceSum += fuelEvent.getVolume() * fuelEvent.getPricePerLitre();
                                volumeSum += fuelEvent.getVolume();
                            }
                        }
                        final float consumption = (100 * fullVolume) / distance;
                        final float priceAvg;
                        if (volumeSum > 0) priceAvg = priceSum / volumeSum;
                        else priceAvg = 0;
                        result.add(new DistanceRecord(carConfiguration.getDistanceUUID(), odometerValue.getDateTaken(), distance));
                        result.add(new VehiclePosition(carConfiguration.getGPSUUID(), odometerValue.getDateTaken(), odometerValue.getLatitude(), odometerValue.getLongitude(), 0));

                        if (carConfiguration.isValidGasolineConsumption(consumption)) {
                            result.add(new VehicleFuelEvent(carConfiguration.getUUID(), odometerValue.getDateTaken(), odometerValue.getOdometerValue(), fullVolume, priceAvg, distance, consumption, odometerValue.getLatitude(), odometerValue.getLongitude()));
                        }
                    }

                }
            }

            for (final VehicleImageData vehicleImageData : otherValues) {
                result.add(new VehiclePosition(carConfiguration.getGPSUUID(), vehicleImageData.getDateTaken(), vehicleImageData.getLatitude(), vehicleImageData.getLongitude(), 0));
            }

        }
        return Ordering.natural().onResultOf(new Function<SensorRecord, Date>() {
            @Nullable
            @Override
            public Date apply(@Nullable SensorRecord sensorRecord) {
                return sensorRecord.getDateTaken().toDate();
            }
        }).sortedCopy(result);
    }

    private static Predicate<VehicleImageData> ConcomittentDateFilter(final VehicleImageData source) {
        final LocalDateTime min = source.getDateTaken().minusMinutes(10);
        final LocalDateTime max = source.getDateTaken().plusMinutes(10);
        return new Predicate<VehicleImageData>() {
            @Override
            public boolean apply(@Nullable VehicleImageData vehicleImageData) {
                return min.isBefore(vehicleImageData.getDateTaken()) && max.isAfter(vehicleImageData.getDateTaken());
            }
        };
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

        final FileOutputStream sqlOutputStream = new FileOutputStream(new File("carsensors.sql"));

        final File storageDir = new File(SystemUtils.getUserDir(), "carsensors");
        if (!storageDir.exists()) storageDir.mkdir();

        final ImageAnnotator.AnnotatedImageBatch batch = JAXBUtils.unmarshal(ImageAnnotator.AnnotatedImageBatch.class, new File(storageDir, "image-annotations.xml"));
        final Iterable<SensorRecord> carSensors = ImageAnalyzer.getFuelStatistics(Iterables.filter(batch.getAnnotatedImages(), DATE_FILTER));
        int lineNumber = 1;
        for (final SensorRecord sensorRecord : carSensors) {
            if (sensorRecord instanceof VehicleFuelEvent) {
                System.out.println(((VehicleFuelEvent) sensorRecord).toCSVLine());
            }
        }
        errorsOut.close();

        sqlOutputStream.close();
    }

    private static final Predicate<AnnotatedImage> DATE_FILTER = new Predicate<AnnotatedImage>() {
        @Override
        public boolean apply(@Nullable AnnotatedImage annotatedImage) {
            return annotatedImage.getDateTaken().isAfter(new LocalDateTime("2015-9-25T00:00:00"));
        }
    };

}
