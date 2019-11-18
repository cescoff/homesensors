package com.desi.data;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.desi.data.bean.TemperatureRecord;
import com.desi.data.impl.StaticSensorNameProvider;
import com.desi.data.spreadsheet.SpreadSheetConverter;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.PropertyConfigurator;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class S3Bridge {

    private static Logger logger = LoggerFactory.getLogger(S3Bridge.class);

    private static final DateTimeFormatter CSV_FORMATTER_1 = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter CSV_FORMATTER_2 = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss");

    private static final String BUCKET_NAME = "desi-sensors";

    private static final String REGION = "eu-west-3";

    private final Iterable<Connector> connectors;

    public S3Bridge(Iterable<Connector> connectors) {
        this.connectors = connectors;
    }

    public boolean sync() {
        if (Iterables.isEmpty(connectors)) {
            logger.error("No connectors provided");
            return false;
        }

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().
                withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("AKIAQ6Y7B4AGYMC2VTPG", "sXLMkvLEAmPd1khHVy4anY+VKBUHTFn0sLNdmhYV"))).
                withRegion(REGION).
                build();
        final ListObjectsV2Result result = s3.listObjectsV2(BUCKET_NAME);

        final List<S3ObjectSummary> objects = result.getObjectSummaries();

        final ImmutableList.Builder<SensorRecord> records = ImmutableList.builder();

        for (S3ObjectSummary os : objects) {
            S3Object fullObject = s3.getObject(new GetObjectRequest(os.getBucketName(), os.getKey()));
            try {
                logger.info("Parsing content for object s3://" + os.getBucketName() + "/" + os.getKey());
                records.addAll(parseContent(fullObject.getObjectContent()));
            } catch (Exception e) {
                logger.error("Failed to parse file s3://" + os.getBucketName() + "/" + os.getKey() + ": " + e.getMessage(), e);
            }
        }

        s3.shutdown();

        for (final Connector connector : connectors) {
            if (!connector.begin()) {
                logger.error("Failed to begin connector '" + connector.getClass().getSimpleName() + "'");
            } else {
                logger.info("Begin of connector '" + connector.getClass().getSimpleName() + "'");
                final Map<String, LocalDateTime> checkpoints = Maps.newHashMap();

                for (final String sensorUUID : getSensorUUIDs(records.build())) {
                    final Optional<LocalDateTime> checkpointValue = connector.getCheckPointValue(sensorUUID);
                    if (checkpointValue.isPresent()) {
                        checkpoints.put(sensorUUID, checkpointValue.get());
                        logger.info("[" + sensorUUID + "] Adding records from checkpoint value for'" + checkpointValue.get() + "'");
                    }
                }


                final Iterable<SensorRecord> filteredSensorRecords = Iterables.filter(records.build(), new Predicate<SensorRecord>() {
                    public boolean apply(SensorRecord sensorRecord) {
                        if (!checkpoints.containsKey(sensorRecord.getSensorUUID())) {
                            return true;
                        } else {
                            return sensorRecord.getDateTaken().isAfter(checkpoints.get(sensorRecord.getSensorUUID()));
                        }
                    }

                    public boolean test(@Nullable SensorRecord input) {
                        return StringUtils.isNotEmpty(input.getSensorUUID());
                    }
                });

                try {
                    if (!connector.addRecords(filteredSensorRecords, new StaticSensorNameProvider())) {
                        logger.error("Failed to add records to connector '" + connector.getClass().getSimpleName() + "'");
                    } else {
                        if (!connector.end()) {
                            logger.error("Failed to end connector '" + connector.getClass().getSimpleName() + "'");
                        }
                    }
                } catch (Exception e) {
                    logger.error("Unexcepted error occured while adding records to connector '" + connector.getClass().getSimpleName() + "'", e);
                }
            }
        }

        return false;
    }

    private Iterable<String> getSensorUUIDs(final Iterable<SensorRecord> records) {
        final Set<String> alreadyAdded = Sets.newHashSet();
        final ImmutableList.Builder<String> result = ImmutableList.builder();
        for (final SensorRecord record : records) {
            if (!alreadyAdded.contains(record.getSensorUUID())) {
                result.add(record.getSensorUUID());
                alreadyAdded.add(record.getSensorUUID());
            }
        }
        return result.build();
    }

    private static Iterable<TemperatureRecord> parseContent(final InputStream content) throws Exception {

        final InputStreamReader inputStreamReader = new InputStreamReader(content);

        final LineIterator lineIterator = new LineIterator(inputStreamReader);

        final ImmutableList.Builder<TemperatureRecord> result = ImmutableList.builder();

        while (lineIterator.hasNext()) {
            final String line = lineIterator.nextLine();
            final String[] elements = StringUtils.split(line, ";");
            if (elements.length >= 4) {
                final String sensorUUID = elements[0];
                final String dateTimeString = elements[1] + " " + elements[2];
                final float value = new Double(StringUtils.remove(elements[3], "C=")).floatValue();

                result.add(new TemperatureRecord(parseDateTime(dateTimeString), value, sensorUUID));
            } else {
                throw new Exception("UNPARSABLE LINE '" + line + "'");
            }
        }

        content.close();

        return result.build();
    }

    private static LocalDateTime parseDateTime(final String dateTimeString) {
        try {
            return CSV_FORMATTER_1.parseDateTime(dateTimeString).toLocalDateTime();
        } catch (Throwable t) {
            return CSV_FORMATTER_2.parseDateTime(dateTimeString).toLocalDateTime();
        }
    }

    public static void main(String[] args) {
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

        new S3Bridge(ImmutableList.<Connector>of(new SpreadSheetConverter())).sync();
    }

}
