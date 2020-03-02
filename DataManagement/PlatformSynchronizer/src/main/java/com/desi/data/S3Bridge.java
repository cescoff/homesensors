package com.desi.data;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.desi.data.aggregation.HeatBurnAggregator;
import com.desi.data.athena.AthenaConnector;
import com.desi.data.bean.HeatBurnSensorRecord;
import com.desi.data.bean.TemperatureRecord;
import com.desi.data.bigquery.BigQueryConnector;
import com.desi.data.config.PlatformCredentialsConfig;
import com.desi.data.csv.S3CSVDataSource;
import com.desi.data.impl.StaticSensorNameProvider;
import com.desi.data.spreadsheet.SpreadSheetConverter;
import com.desi.data.utils.JAXBUtils;
import com.desi.data.utils.LogUtils;
import com.desi.data.utils.TemperatureCSVParser;
import com.desi.data.zoho.ZohoFileConnector;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.PropertyConfigurator;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class S3Bridge {

    private static Logger logger = LoggerFactory.getLogger(S3Bridge.class);

    private static final String BUCKET_NAME = "desi-sensors";

    private static final String REGION = "eu-west-3";

    private static final String DEFAULT_OWNER_EMAIL = "corentin.escoffier@gmail.com";

    private final DataSource dataSource;

    private final Iterable<Connector> connectors;

    private final File awsCredentialsConfigurationFile;

    private final PlatformClientId clientId;

    private final String folder;

    private PlatformCredentialsConfig credentialsConfig;

    private String accessKey;

    private String secretKey;

    private AtomicBoolean INIT_DONE = new AtomicBoolean(false);

    private SensorNameProvider sensorNameProvider = null;

    private Iterable<DataSource> connectedDataSources = Lists.newArrayList();

    public S3Bridge(DataSource dataSource, Iterable<Connector> connectors, File awsCredentialsConfigurationFile, PlatformClientId clientId, final String folder) {
        this.dataSource = dataSource;
        this.connectors = connectors;
        this.awsCredentialsConfigurationFile = awsCredentialsConfigurationFile;
        this.clientId = clientId;
        this.folder = folder;
        this.sensorNameProvider = new StaticSensorNameProvider();
    }


    private synchronized void init() {
        if (INIT_DONE.get()) {
            return;
        }
        if (!this.awsCredentialsConfigurationFile.exists()) {
            throw new IllegalStateException("Credentials file '" + this.awsCredentialsConfigurationFile.getPath() + "' does not exist");
        }

        try {
            credentialsConfig = JAXBUtils.unmarshal(PlatformCredentialsConfig.class, this.awsCredentialsConfigurationFile);
        } catch (Throwable t) {
            throw new IllegalStateException("Malformed file '" + this.awsCredentialsConfigurationFile.getPath() + "'", t);
        }
        for (final PlatformCredentialsConfig.Credentials credentials : credentialsConfig.getCredentials()) {
            if (credentials.getId() == this.clientId) {
                this.accessKey = credentials.getAccessKey();
                this.secretKey = credentials.getSecretKey();
                this.INIT_DONE.set(true);
                return;
            }
        }
        throw new IllegalStateException("No service '" + this.clientId + "' configured into file '" + this.awsCredentialsConfigurationFile.getPath() + "'");
    }


    public boolean sync() {
        if (Iterables.isEmpty(connectors)) {
            logger.error("No connectors provided");
            return false;
        }

        init();

        if (dataSource != null) {
            final PlatformCredentialsConfig.Credentials credentials;
            if (dataSource.getPlatformId().isPresent()) {
                PlatformCredentialsConfig.Credentials candidate = null;
                for (final PlatformCredentialsConfig.Credentials configuredCredentials : this.credentialsConfig.getCredentials()) {
                    if (configuredCredentials.getId() == dataSource.getPlatformId().get()) {
                        candidate = configuredCredentials;
                    }
                }
                if (candidate == null) {
                    logger.error("No credentials found for connector '"
                            + dataSource.getClass().getSimpleName()
                            + "' with service '"
                            + dataSource.getPlatformId().get()
                            + "' in file '"
                            + this.awsCredentialsConfigurationFile.getPath()
                            + "'");
                    return false;
                }
                credentials = candidate;
            } else {
                credentials = null;
            }
            if (!dataSource.begin(credentials, awsCredentialsConfigurationFile.getParentFile())) {
                logger.error("Failed to begin connector '" + dataSource.getClass().getSimpleName() + "'");
                return false;
            }
        }

        for (final Connector connector : connectors) {
            final PlatformCredentialsConfig.Credentials credentials;
            if (connector.getPlatformId().isPresent()) {
                PlatformCredentialsConfig.Credentials candidate = null;
                for (final PlatformCredentialsConfig.Credentials configuredCredentials : this.credentialsConfig.getCredentials()) {
                    if (configuredCredentials.getId() == connector.getPlatformId().get()) {
                        candidate = configuredCredentials;
                    }
                }
                if (candidate == null) {
                    logger.error("No credentials found for connector '"
                            + connector.getClass().getSimpleName()
                            + "' with service '"
                            + connector.getPlatformId().get()
                            + "' in file '"
                            + this.awsCredentialsConfigurationFile.getPath()
                            + "'");
                    return false;
                }
                credentials = candidate;
            } else {
                credentials = null;
            }
            if (!connector.begin(credentials, awsCredentialsConfigurationFile.getParentFile())) {
                logger.error("Failed to begin connector '" + connector.getClass().getSimpleName() + "'");
            } else {
                logger.info("Begin of connector '" + connector.getClass().getSimpleName() + "'");

                final Iterable<String> sensorUUIDs = dataSource.getSensorsUUIDs();

                final Map<String, LocalDateTime> checkpoints = Maps.newHashMap();

                for (final String sensorUUID : sensorUUIDs) {
                    final Optional<LocalDateTime> checkpointValue = connector.getCheckPointValue(sensorUUID);
                    if (checkpointValue.isPresent()) {
                        checkpoints.put(sensorUUID, checkpointValue.get());
                        String sensorName = this.sensorNameProvider.getDisplayName(sensorUUID);
                        if (connector instanceof SensorNameProvider) {
                            sensorName = ((SensorNameProvider) connector).getDisplayName(sensorUUID);
                        }
                        logger.info(sensorName + "[" + sensorUUID + "] Adding records from checkpoint value for'" + checkpointValue.get() + "'");
                    } else {
                        logger.info("No checkpoint for " + this.sensorNameProvider.getDisplayName(sensorUUID) + "[" + sensorUUID + "]");
                    }
                }

                final ImmutableList.Builder<SensorRecord> records2Add = ImmutableList.builder();

                if (StringUtils.isNotEmpty(this.sensorNameProvider.getBurnerUUID(DEFAULT_OWNER_EMAIL))) {
                    final Map<String, Iterable<SensorRecord>> recordsBySensorUUID = Maps.newHashMap();
                    for (final String sensorUUID : sensorUUIDs) {
                        if (this.sensorNameProvider.getType(sensorUUID) ==  SensorType.HEATING_TEMPERATURE) {
                            LocalDateTime checkPoint = checkpoints.get(sensorUUID);
                            if (checkPoint == null) {
                                checkPoint = LocalDate.now().withYear(2019).withMonthOfYear(11).withDayOfMonth(1).toDateTimeAtStartOfDay().toLocalDateTime();
                            } else {
                                checkPoint = LocalDateTime.now().minusDays(11);
                            }
                            recordsBySensorUUID.put(sensorUUID, dataSource.getRecords(sensorUUID, checkPoint));
                        }
                    }

                    final Map<String, List<SensorRecord>> newRecordsBySensorUUID = Maps.newHashMap();
                    for (final String sensorUUID : recordsBySensorUUID.keySet()) {
                        final SensorType sensorType = this.sensorNameProvider.getType(sensorUUID);
                        if (sensorType == SensorType.HEATING_TEMPERATURE) {
                            final String heatBurnSensorUUID = this.sensorNameProvider.getBurnerUUID(DEFAULT_OWNER_EMAIL);
                            if (!newRecordsBySensorUUID.containsKey(heatBurnSensorUUID)) {
                                newRecordsBySensorUUID.put(heatBurnSensorUUID, Lists.newArrayList());
                            }
                            final HeatBurnAggregator heatBurnAggregator = new HeatBurnAggregator(LocalDateTime.now().minusDays(10), LocalDateTime.now(), this.sensorNameProvider);
                            for (final SensorRecord sensorRecord : recordsBySensorUUID.get(sensorUUID)) {
                                heatBurnAggregator.add(sensorRecord);
                            }
                            final Iterable<SensorRecord> heatBurnsSensorRecords = heatBurnAggregator.compute().get(sensorUUID);
                            if (heatBurnsSensorRecords != null) {
                                Iterables.addAll(newRecordsBySensorUUID.get(heatBurnSensorUUID), heatBurnsSensorRecords);
                            }
                        }

                    }
                    for (final String newSensorUUID : newRecordsBySensorUUID.keySet()) {
                        records2Add.addAll(newRecordsBySensorUUID.get(newSensorUUID));
                        if (!recordsBySensorUUID.containsKey(newSensorUUID)) {
                            recordsBySensorUUID.put(newSensorUUID, newRecordsBySensorUUID.get(newSensorUUID));
                        } else {
                            final List<SensorRecord> addedValues = newRecordsBySensorUUID.get(newSensorUUID);
                            Iterables.addAll(addedValues, recordsBySensorUUID.get(newSensorUUID));
                            recordsBySensorUUID.put(newSensorUUID, addedValues);
                        }
                    }
                }

                try {
                    if (!connector.addRecords(records2Add.build(), this.sensorNameProvider)) {
                        logger.warn("No record sent by connector '" + connector.getClass().getSimpleName() + "'");
                    }
                } catch (Throwable t) {
                    logger.error("Unexcepted error occured while adding records to connector '" + connector.getClass().getSimpleName() + "'", t);
                }

                for (final String uuid : checkpoints.keySet()) {
                    try {
                        if (!connector.addRecords(dataSource.getRecords(uuid, checkpoints.get(uuid)), this.sensorNameProvider)) {
                            logger.warn("No record sent by connector '" + connector.getClass().getSimpleName() + "'");
                        } else {
                            if (!connector.end()) {
                                logger.error("Failed to end connector '" + connector.getClass().getSimpleName() + "'");
                            }
                        }
                    } catch (Exception e) {
                        logger.error("Unexcepted error occured while adding records to connector '" + connector.getClass().getSimpleName() + "'", e);
                    }
                }

                for (final String uuid : sensorUUIDs) {
                    if (!checkpoints.containsKey(uuid)) {
                        try {
                            final LocalDateTime checkPoint = LocalDate.now().withYear(2000).withDayOfMonth(1).withMonthOfYear(1).toDateTimeAtStartOfDay().toLocalDateTime();
                            if (!connector.addRecords(dataSource.getRecords(uuid, checkPoint), this.sensorNameProvider)) {
                                logger.warn("No record sent by connector '" + connector.getClass().getSimpleName() + "'");
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

            }
        }

        return true;
    }

    private static final Function<SensorRecord, LocalDateTime> SENSORRECORD_SORT = new Function<SensorRecord, LocalDateTime>() {

        @Nullable
        @Override
        public LocalDateTime apply(@Nullable SensorRecord sensorRecord) {
            return sensorRecord.getDateTaken();
        }
    };

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

    private Map<String, Iterable<SensorRecord>> getSensorRecordsByUUID(final Iterable<SensorRecord> records) {
        final Map<String, Iterable<SensorRecord>> index = Maps.newHashMap();

        for (final SensorRecord sensorRecord : records) {
            if (!index.containsKey(sensorRecord.getSensorUUID())) {
                index.put(sensorRecord.getSensorUUID(), Lists.newArrayList());
            }
            ((List<SensorRecord>) index.get(sensorRecord.getSensorUUID())).add(sensorRecord);
        }
        return index;
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage " + S3Bridge.class.getSimpleName() + " <CREDENTIALS_FILE_PATH>");
            System.exit(2);
            return;
        }

        LogUtils.configure("synchronizer.log");


        try {
/*            if (!new S3Bridge(ImmutableList.<Connector>of(new SpreadSheetConverter()), new File(args[0]), PlatformClientId.S3Bridge).sync()) {
                logger.warn("Synchronization process returned any data synchronized");
                System.exit(4);
            }*/
            if (!new S3Bridge(new AthenaConnector(new StaticSensorNameProvider()), ImmutableList.<Connector>of(new BigQueryConnector("Records")/*, new SpreadSheetConverter()*/), new File(args[0]), PlatformClientId.S3Bridge, "peri").sync()) {
                logger.warn("Synchronization process returned any data synchronized");
                System.exit(4);
            }
            if (!new S3Bridge(new S3CSVDataSource(new StaticSensorNameProvider(), "geanges"), ImmutableList.<Connector>of(new BigQueryConnector("GeangesRecords")/*, new SpreadSheetConverter()*/), new File(args[0]), PlatformClientId.S3Bridge, "geanges").sync()) {
                logger.warn("Synchronization process returned any data synchronized");
                System.exit(4);
            }
        } catch (Throwable t) {
            logger.error("An error has occured", t);
            System.exit(4);
        }
        System.exit(1);
    }

}
