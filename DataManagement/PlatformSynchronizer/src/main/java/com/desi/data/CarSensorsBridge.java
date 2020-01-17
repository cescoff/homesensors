package com.desi.data;

import com.desi.data.bean.*;
import com.desi.data.bigquery.BigQueryConnector;
import com.desi.data.config.CarConfiguration;
import com.desi.data.config.ConfigurationUtils;
import com.desi.data.utils.CarConfigurationHelper;
import com.desi.data.utils.ImageAnalyzer;
import com.desi.data.utils.JAXBUtils;
import com.google.cloud.bigquery.BigQueryError;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.PropertyConfigurator;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.javatuples.Pair;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.*;

public class CarSensorsBridge {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage " + S3Bridge.class.getSimpleName() + " <CREDENTIALS_FILE_PATH>");
            System.exit(2);
            return;
        }
        final Properties logConfig = new Properties();

        logConfig.setProperty("log4j.rootLogger", "INFO, Appender1,Appender2");
        logConfig.setProperty("log4j.appender.Appender1", "org.apache.log4j.ConsoleAppender");
        logConfig.setProperty("log4j.appender.Appender1.layout", "org.apache.log4j.PatternLayout");
        logConfig.setProperty("log4j.appender.Appender1.layout.ConversionPattern", "%-7p %d [%t] %c %x - %m%n");
        logConfig.setProperty("log4j.appender.Appender2", "org.apache.log4j.FileAppender");
        logConfig.setProperty("log4j.appender.Appender2.File", "logs/carsynchronizer.log");
        logConfig.setProperty("log4j.appender.Appender2.layout", "org.apache.log4j.PatternLayout");
        logConfig.setProperty("log4j.appender.Appender2.layout.ConversionPattern", "%-7p %d [%t] %c %x - %m%n");

        PropertyConfigurator.configure(logConfig);

        final Logger logger = LoggerFactory.getLogger(CarSensorsBridge.class);

        try {
/*            if (!new S3Bridge(ImmutableList.<Connector>of(new SpreadSheetConverter()), new File(args[0]), PlatformClientId.S3Bridge).sync()) {
                logger.warn("Synchronization process returned any data synchronized");
                System.exit(4);
            }*/

            final ImageAnnotator imageAnnotator = new ImageAnnotator(Lists.newArrayList("desi-car-images", "desi-car-fuel", "desi-roadtripimages"), new File(args[0]), ConfigurationUtils.getAnnotationsFile(), PlatformClientId.S3Bridge);

            final Iterable<AnnotatedImage> annotatedImages = imageAnnotator.annotate(LocalDateTime.parse("2010-1-1T00:00:00"), true);
            logger.info("Now analyzing images");
            final Iterable<SensorRecord> carSensors = ImageAnalyzer.getFuelStatistics(annotatedImages);
            for (final SensorRecord sensorRecord : carSensors) {
                if (sensorRecord instanceof VehicleFuelEvent) {
                    logger.info("FuelEvent: " + ((VehicleFuelEvent) sensorRecord).toCSVLine());
                }
            }

            for (final SensorRecord sensorRecord : carSensors) {
                if (!(sensorRecord instanceof VehicleFuelEvent) && !(sensorRecord instanceof VehiclePosition)) {
                    logger.info("Car sensor[" + sensorRecord.getSensorUUID() + "]:" + sensorRecord.getDateTaken() + "," + sensorRecord.getValue() + " " + sensorRecord.getUnit().getDisplay());
                }
            }

            final List<Pair<VehiclePosition, VehiclePosition>> trips = Lists.newArrayList();

            VehiclePosition tripBegin = null;
            VehiclePosition lastTripPosition = null;
            for (final VehiclePosition vehiclePosition : Ordering.natural().onResultOf(new Function<VehiclePosition, Date>() {

                @Nullable
                @Override
                public Date apply(@Nullable VehiclePosition vehiclePosition) {
                    return vehiclePosition.getDateTaken().toDate();
                }

            }).sortedCopy(Iterables.filter(carSensors, VehiclePosition.class))) {
                final CarConfiguration carConfiguration = CarConfigurationHelper.resolveUUID(vehiclePosition.getSensorUUID());
                if (carConfiguration.isVehicleInATrip(vehiclePosition)) {
                    if (tripBegin == null) {
                        tripBegin = vehiclePosition;
                    } else {
                        lastTripPosition = vehiclePosition;
                    }
                } else {
                    if (lastTripPosition == null) {
                        tripBegin = null;
                    } else {
                        if (tripBegin.getSensorUUID().equals(lastTripPosition.getSensorUUID())) {
                            logger.info("Adding trip starting at " + tripBegin.getDateTaken() + ", ending at " + lastTripPosition.getDateTaken());
                            trips.add(Pair.with(tripBegin, lastTripPosition));
                        }
                        tripBegin = null;
                        lastTripPosition = null;
                    }
                }
            }

            final List<SensorRecord> sensorsWithTripImages = Lists.newArrayList(carSensors);
            logger.info("Found " + trips.size() + " trips");
            for (final AnnotatedImage annotatedImage : annotatedImages) {
                for (final Pair<VehiclePosition, VehiclePosition> trip : trips) {
                    if (trip.getValue0().getDateTaken().isBefore(annotatedImage.getDateTaken())
                        && annotatedImage.getDateTaken().isBefore(trip.getValue1().getDateTaken())) {
                        final VehicleImageData imageData = new VehicleImageData() {
                            @Override
                            public String getUUID() {
                                return trip.getValue0().getSensorUUID();
                            }

                            @Override
                            public float getOdometerValue() {
                                return 0;
                            }

                            @Override
                            public float getVolume() {
                                return 0;
                            }

                            @Override
                            public float getPrice() {
                                return 0;
                            }

                            @Override
                            public float getPricePerLitre() {
                                return 0;
                            }

                            @Override
                            public boolean hasVolume() {
                                return false;
                            }

                            @Override
                            public boolean hasPrice() {
                                return false;
                            }

                            @Override
                            public boolean hasPricePerLitre() {
                                return false;
                            }

                            @Override
                            public boolean hasOdometerValue() {
                                return false;
                            }

                            @Override
                            public String getFileName() {
                                return null;
                            }

                            @Override
                            public LocalDateTime getDateTaken() {
                                return annotatedImage.getDateTaken();
                            }

                            @Override
                            public float getLatitude() {
                                if (StringUtils.isEmpty(annotatedImage.getLatitude())) {
                                    return 0;
                                }
                                return new GPSLatitudeSensorRecord(
                                        trip.getValue0().getSensorUUID(),
                                        annotatedImage.getDateTaken(),
                                        annotatedImage.getLatitudeRef(),
                                        annotatedImage.getLatitude()).getValue();
                            }

                            @Override
                            public float getLongitude() {
                                if (StringUtils.isEmpty(annotatedImage.getLatitude())) {
                                    return 0;
                                }
                                return new GPSLongitudeSensorRecord(
                                        trip.getValue0().getSensorUUID(),
                                        annotatedImage.getDateTaken(),
                                        annotatedImage.getLongitudeRef(),
                                        annotatedImage.getLongitude()).getValue();
                            }

                            @Override
                            public float getAltitude() {
                                if (StringUtils.isEmpty(annotatedImage.getAltitude())) {
                                    return 0;
                                }
                                if (StringUtils.contains(annotatedImage.getAltitude(), " metres")) {
                                    return Float.parseFloat(StringUtils.remove(annotatedImage.getAltitude(), " metres"));
                                }
                                return 0;
                            }

                            @Override
                            public VehicleImageData getImageData() {
                                return this;
                            }

                            @Override
                            public float getValue() {
                                return 0;
                            }

                            @Override
                            public String getSensorUUID() {
                                return trip.getValue0().getSensorUUID();
                            }

                            @Override
                            public SensorUnit getUnit() {
                                return SensorUnit.POSITION;
                            }
                        };
                        logger.info("Adding trip image at date time " + imageData.getDateTaken());
                        sensorsWithTripImages.add(new VehiclePosition(
                                imageData,
                                trip.getValue0().getSensorUUID(),
                                annotatedImage.getFileName(),
                                annotatedImage.getDateTaken(),
                                imageData.getLatitude(),
                                imageData.getLongitude(),
                                imageData.getAltitude(),
                                0));
                    }
                }
            }


            final BigQueryConnector bigQueryConnector = new BigQueryConnector("CarSensors");
            bigQueryConnector.begin(imageAnnotator.getGoogleCloudCredentials(), imageAnnotator.getConfigDir());

            final Map<String, LocalDateTime> checkPoints = Maps.newHashMap();
            final Set<String> newSensorIds = Sets.newHashSet();
            for (final SensorRecord sensorRecord : sensorsWithTripImages) {
                if (!checkPoints.containsKey(sensorRecord.getSensorUUID()) && !newSensorIds.contains(sensorRecord.getSensorUUID())) {
                    final Optional<LocalDateTime> checkPoint = bigQueryConnector.getCheckPointValue(sensorRecord.getSensorUUID());
                    if (checkPoint.isPresent()) {
                        checkPoints.put(sensorRecord.getSensorUUID(), checkPoint.get());
                    } else {
                        newSensorIds.add(sensorRecord.getSensorUUID());
                    }
                }
            }

            if (bigQueryConnector.addRecords(Iterables.filter(sensorsWithTripImages, DateTimeFilter(checkPoints)), CarConfigurationHelper.getInstance())) {
                logger.info("Car statistics recorded");
            } else {
                logger.error("Failed to record car statistics");
            }
        } catch (Throwable t) {
            logger.error("An error has occured", t);
            System.exit(4);
        }
        System.exit(1);

    }

    private static Predicate<SensorRecord> DateTimeFilter(final Map<String, LocalDateTime> checkPoints) {
        return new Predicate<SensorRecord>() {
            @Override
            public boolean apply(@Nullable SensorRecord sensorRecord) {
                if (!checkPoints.containsKey(sensorRecord.getSensorUUID())) {
                    return true;
                }
                return sensorRecord.getDateTaken().isAfter(checkPoints.get(sensorRecord.getSensorUUID()));
            }
        };
    }

}
