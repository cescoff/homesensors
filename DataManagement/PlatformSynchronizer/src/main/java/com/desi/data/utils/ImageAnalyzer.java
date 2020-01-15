package com.desi.data.utils;

import com.desi.data.CarSensorRecord;
import com.desi.data.ImageAnnotator;
import com.desi.data.SensorRecord;
import com.desi.data.SensorUnit;
import com.desi.data.bean.*;
import com.desi.data.config.CarConfiguration;
import com.desi.data.config.ConfigurationUtils;
import com.desi.data.config.FuelOCRParser;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.log4j.PropertyConfigurator;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.Period;
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
                            && !fuelStatistics.hasPrice() && !fuelStatistics.hasPricePerLitre()
                            && fuelStatistics.getLatitude() != 0
                            && fuelStatistics.getLongitude() != 0;
                }
            }));

            final Map<VehicleImageData, List<VehicleImageData>> fuelEvents = Maps.newHashMap();
            VehicleImageData previousOdometerValue = null;
            List<VehicleImageData> currentBuffer = Lists.newArrayList();
            for (final VehicleImageData fuelValue : fuelValues) {
                final Iterable<VehicleImageData> odometers = Iterables.filter(odometerValues, ConcomittentDateFilter(fuelValue));
                if (!Iterables.isEmpty(odometers)) {
                    if (currentBuffer.size() > 0) {
//
                        if (previousOdometerValue != null) {
                            fuelEvents.get(previousOdometerValue).addAll(currentBuffer);
                            currentBuffer = Lists.newArrayList();
                            previousOdometerValue = null;
                        } else {
                            logger.error("No suitable odometer value found for event " + fuelValue.getDateTaken() + ", fileName='" + fuelValue.getImageData().getFileName() + "'");
                        }
                    }

                    previousOdometerValue = Iterables.getFirst(odometers, null);
                    fuelEvents.put(previousOdometerValue, Lists.newArrayList(fuelValue));
                } else {
                    currentBuffer.add(fuelValue);
                }
            }
            if (previousOdometerValue != null && currentBuffer.size() > 0) {
                fuelEvents.put(previousOdometerValue, currentBuffer);
            }

            for (int index = 0; index < odometerValues.size(); index++) {
                final VehicleImageData odometerValue = odometerValues.get(index);
                final OdometerRecord odometerRecord = new OdometerRecord(
                        odometerValue,
                        carConfiguration.getOdometerUUID(),
                        odometerValue.getDateTaken(),
                        odometerValue.getOdometerValue());
                result.add(odometerRecord);
                if (fuelEvents.containsKey(odometerValue) && index < (odometerValues.size() - 1)) {
                    final VehicleImageData previousValue = odometerValues.get(index + 1);;
                    final float distance = odometerValue.getOdometerValue() - previousValue.getOdometerValue();

                    if (distance > 0) {
                        float fullVolume = 0;
                        float priceSum = 0;
                        float volumeSum = 0;

                        for (final VehicleImageData fuelEvent : fuelEvents.get(odometerValue)) {
                            if (fuelEvent.hasPricePerLitre()) {
                                result.add(new PriceRecord(
                                        fuelEvent,
                                        carConfiguration.getGasolineUUID(odometerRecord, odometerRecord),
                                        odometerValue.getDateTaken(),
                                        fuelEvent.getPricePerLitre(),
                                        SensorUnit.EURO));
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

                        result.add(new DistanceRecord(odometerValue, carConfiguration.getDistanceUUID(previousValue, odometerRecord), odometerValue.getDateTaken(), distance));
                        result.add(new VehiclePosition(carConfiguration.getGPSUUID(), odometerValue.getFileName(), odometerValue.getDateTaken(), odometerValue.getLatitude(), odometerValue.getLongitude(), 0));

                        if (carConfiguration.isValidGasolineConsumption(consumption)) {
                            result.add(new VehicleFuelEvent(
                                    previousValue,
                                    odometerValue,
                                    carConfiguration.getUUID(),
                                    odometerValue.getDateTaken(),
                                    odometerValue.getOdometerValue(),
                                    fullVolume,
                                    priceAvg,
                                    distance,
                                    consumption));
                        }
                    }

                }
            }

            if (carConfiguration.getName().equals("Volkswagen T3")) {
                logger.info("COMBI");
            }

            for (final VehicleImageData vehicleImageData : otherValues) {
                if (vehicleImageData.getLatitude() != 0 && vehicleImageData.getLongitude() != 0) {
                    result.add(new VehiclePosition(carConfiguration.getGPSUUID(), vehicleImageData.getFileName(), vehicleImageData.getDateTaken(), vehicleImageData.getLatitude(), vehicleImageData.getLongitude(), 0));
                }
            }

        }
        return Ordering.natural().onResultOf(new Function<SensorRecord, Date>() {
            @Nullable
            @Override
            public Date apply(@Nullable SensorRecord sensorRecord) {
                return sensorRecord.getDateTaken().toDate();
            }
        }).sortedCopy(linearize(result));
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

    private static Iterable<SensorRecord> linearize(final Iterable<SensorRecord> records) {
        final Map<String, List<OdometerRecord>> initialOdometerValues = Maps.newHashMap();
        final Map<String, List<PriceRecord>> initialPriceRecords = Maps.newHashMap();
        final Map<String, List<VehicleFuelEvent>> initialFuelEvents = Maps.newHashMap();

        for (final CarConfiguration carConfiguration : CarConfigurationHelper.all()) {
            initialOdometerValues.put(carConfiguration.getUUID(), Lists.newArrayList());
            initialPriceRecords.put(carConfiguration.getUUID(), Lists.newArrayList());
            initialFuelEvents.put(carConfiguration.getUUID(), Lists.newArrayList());
        }

        final Iterable<SensorRecord> otherValues = Iterables.filter(records, new Predicate<SensorRecord>() {
            @Override
            public boolean apply(@Nullable SensorRecord sensorRecord) {
                if (sensorRecord instanceof DistanceRecord) {
                    return false;
                }
                if (sensorRecord instanceof PriceRecord) {
                    return false;
                }

                return true;
            }
        });

        for (final SensorRecord record : records) {
            if (record instanceof VehicleFuelEvent) {
                final String carUUID = CarConfigurationHelper.resolveUUID(record.getSensorUUID()).getUUID();
                initialFuelEvents.get(carUUID).add((VehicleFuelEvent) record);
            }
            if (record instanceof PriceRecord) {
                final String carUUID = CarConfigurationHelper.resolveUUID(record.getSensorUUID()).getUUID();
                initialPriceRecords.get(carUUID).add((PriceRecord) record);
            }
            if (record instanceof OdometerRecord) {
                final String carUUID = CarConfigurationHelper.resolveUUID(record.getSensorUUID()).getUUID();
                initialOdometerValues.get(carUUID).add((OdometerRecord) record);
            }
        }

        final List<SensorRecord> result = Lists.newArrayList(otherValues);

        for (final String carUUID : initialOdometerValues.keySet()) {
            final CarConfiguration carConfiguration = CarConfigurationHelper.resolveUUID(carUUID);
            logger.info("Handling odometer values for vehicle '" + carConfiguration.getName() + "'");

            final Map<LocalDate, List<OdometerRecord>> valuesPerDay = Maps.newHashMap();
            for (final OdometerRecord odometerRecord : initialOdometerValues.get(carUUID)) {
                if (!valuesPerDay.containsKey(odometerRecord.getDateTaken().toLocalDate())) {
                    valuesPerDay.put(odometerRecord.getDateTaken().toLocalDate(), Lists.newArrayList());
                }
                valuesPerDay.get(odometerRecord.getDateTaken().toLocalDate()).add(odometerRecord);
            }

            LocalDate previous = null;
            List<OdometerRecord> previousOrderedRecords = null;
            for (final LocalDate localDate : Ordering.natural().sortedCopy(valuesPerDay.keySet())) {
                final List<OdometerRecord> orderedDayRecords = Ordering.natural().onResultOf(SORTER).sortedCopy(valuesPerDay.get(localDate));
                if (valuesPerDay.get(localDate).size() > 1) {
                    result.add(new DistanceRecord(
                            orderedDayRecords.get(orderedDayRecords.size() - 1).getImageData(),
                            carConfiguration.getDistanceUUID(orderedDayRecords.get(0), orderedDayRecords.get(orderedDayRecords.size() - 1)),
                            localDate.toDateTimeAtStartOfDay().toLocalDateTime(),
                            orderedDayRecords.get(orderedDayRecords.size() - 1).getValue() - orderedDayRecords.get(0).getValue()));
                }
                if (previous != null) {
                    final float distance = orderedDayRecords.get(0).getValue() - previousOrderedRecords.get(previousOrderedRecords.size() - 1).getValue();
                    int numberOfDays = 0;
                    LocalDate increment = previous;
                    while (increment.isBefore(localDate)) {
                        numberOfDays++;
                        increment = increment.plusDays(1);
                    }
                    if (orderedDayRecords.size() == 1) {
                        numberOfDays++;
                    }
                    for (int index = 1; index <= numberOfDays; index++) {
                        result.add(new DistanceRecord(
                                orderedDayRecords.get(0).getImageData(),
                                carConfiguration.getDistanceUUID(previousOrderedRecords.get(previousOrderedRecords.size() - 1), orderedDayRecords.get(0)),
                                previous.plusDays(index).toDateTimeAtStartOfDay().toLocalDateTime(),
                                distance / numberOfDays));
                    }
                }
                previous = localDate;
                previousOrderedRecords = orderedDayRecords;
            }

        }

        for (final String carUUID : initialPriceRecords.keySet()) {
            final CarConfiguration carConfiguration = CarConfigurationHelper.resolveUUID(carUUID);
            logger.info("Handling price values for vehicle '" + CarConfigurationHelper.resolveUUID(carUUID).getName() + "'");
            final Iterable<PriceRecord> sortedValues = Ordering.natural().onResultOf(SORTER).sortedCopy(initialPriceRecords.get(carUUID));

            PriceRecord previous = null;
            for (final PriceRecord priceRecord : sortedValues) {
                if (previous != null) {
                    LocalDateTime current = previous.getDateTaken();
                    while (current.isBefore(priceRecord.getDateTaken())) {
                        result.add(new PriceRecord(
                                priceRecord.getImageData(),
                                carConfiguration.getGasolineUUID(priceRecord, priceRecord),
                                current,
                                previous.getValue(),
                                SensorUnit.EURO));
                        current = current.plusDays(1);
                    }
                }
                previous = priceRecord;
            }
        }

        for (final String carUUID : initialFuelEvents.keySet()) {
            final CarConfiguration carConfiguration = CarConfigurationHelper.resolveUUID(carUUID);
            logger.info("Handling Fuel events for vehicle '" + CarConfigurationHelper.resolveUUID(carUUID).getName() + "'");
            final Iterable<VehicleFuelEvent> sortedValues = Ordering.natural().onResultOf(SORTER).sortedCopy(initialFuelEvents.get(carUUID));

            VehicleFuelEvent previous = null;
            for (final VehicleFuelEvent vehicleFuelEvent : sortedValues) {
                if (previous != null) {
                    LocalDateTime current = previous.getDateTaken();
                    while (current.isBefore(vehicleFuelEvent.getDateTaken())) {
                        result.add(new GasolineConsumptionRecord(
                                vehicleFuelEvent.getImageData(),
                                carConfiguration.getGasolineAvgConsumptionUUID(vehicleFuelEvent.getStartImageData(), vehicleFuelEvent.getEndImageData()),
                                current,
                                vehicleFuelEvent.getValue()));
                        current = current.plusDays(1);
                    }
                }
                previous = vehicleFuelEvent;
            }
        }


        return ImmutableList.copyOf(result);
    }

    private static Function<SensorRecord, Date> SORTER = new Function<SensorRecord, Date>() {
        @Nullable
        @Override
        public Date apply(@Nullable SensorRecord sensorRecord) {
            return sensorRecord.getDateTaken().toDate();
        }
    };

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


        final ImageAnnotator.AnnotatedImageBatch batch = JAXBUtils.unmarshal(ImageAnnotator.AnnotatedImageBatch.class, ConfigurationUtils.getAnnotationsFile());
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
