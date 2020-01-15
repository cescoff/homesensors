package com.desi.data;

import com.desi.data.bean.AnnotatedImage;
import com.desi.data.bean.VehicleFuelEvent;
import com.desi.data.bean.VehiclePosition;
import com.desi.data.bigquery.BigQueryConnector;
import com.desi.data.config.ConfigurationUtils;
import com.desi.data.utils.CarConfigurationHelper;
import com.desi.data.utils.ImageAnalyzer;
import com.desi.data.utils.JAXBUtils;
import com.google.cloud.bigquery.BigQueryError;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.log4j.PropertyConfigurator;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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
        logConfig.setProperty("log4j.appender.Appender2.File", "logs/synchronizer.log");
        logConfig.setProperty("log4j.appender.Appender2.layout", "org.apache.log4j.PatternLayout");
        logConfig.setProperty("log4j.appender.Appender2.layout.ConversionPattern", "%-7p %d [%t] %c %x - %m%n");

        PropertyConfigurator.configure(logConfig);

        final Logger logger = LoggerFactory.getLogger(CarSensorsBridge.class);

        try {
/*            if (!new S3Bridge(ImmutableList.<Connector>of(new SpreadSheetConverter()), new File(args[0]), PlatformClientId.S3Bridge).sync()) {
                logger.warn("Synchronization process returned any data synchronized");
                System.exit(4);
            }*/

            final ImageAnnotator imageAnnotator = new ImageAnnotator(Lists.newArrayList("desi-car-images", "desi-car-fuel"), new File(args[0]), ConfigurationUtils.getAnnotationsFile(), PlatformClientId.S3Bridge);

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

            final BigQueryConnector bigQueryConnector = new BigQueryConnector("CarSensors");
            bigQueryConnector.begin(imageAnnotator.getGoogleCloudCredentials(), imageAnnotator.getConfigDir());

            final Map<String, LocalDateTime> checkPoints = Maps.newHashMap();
            final Set<String> newSensorIds = Sets.newHashSet();
            for (final SensorRecord sensorRecord : carSensors) {
                if (!checkPoints.containsKey(sensorRecord.getSensorUUID()) && !newSensorIds.contains(sensorRecord.getSensorUUID())) {
                    final Optional<LocalDateTime> checkPoint = bigQueryConnector.getCheckPointValue(sensorRecord.getSensorUUID());
                    if (checkPoint.isPresent()) {
                        checkPoints.put(sensorRecord.getSensorUUID(), checkPoint.get());
                    } else {
                        newSensorIds.add(sensorRecord.getSensorUUID());
                    }
                }
            }

            if (bigQueryConnector.addRecords(Iterables.filter(carSensors, DateTimeFilter(checkPoints)), CarConfigurationHelper.getInstance())) {
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
