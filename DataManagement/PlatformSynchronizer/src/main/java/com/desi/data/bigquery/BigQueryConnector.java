package com.desi.data.bigquery;

import com.desi.data.*;
import com.desi.data.bean.DefaultAggregatedSensorRecord;
import com.desi.data.bean.DefaultHeatingLevelRecord;
import com.desi.data.bean.TemperatureRecord;
import com.desi.data.config.PlatformCredentialsConfig;
import com.desi.data.impl.StaticSensorNameProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import org.apache.commons.lang.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.javatuples.Pair;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BigQueryConnector implements Connector {

    private static final Logger logger = LoggerFactory.getLogger(BigQueryConnector.class);

    private static final LocalDateTime DEFAULT_CHECKPOINT_VALUE = LocalDateTime.parse("2019-11-4T00:00:00");

    private static final String DATASET_NAME_PARAMETER = "${DATASET}";

    private static final String SENSOR_RECORDS_TABLE_NAME = "SensorData";

    private static final String AGGREGATED_SENSOR_RECORDS_TABLE_NAME = "SensorDataAggregated";

    private static final String HEATING_LEVEL_TABLE_NAME = "HeatingLevel";

    private static final String SENSOR_ID_QUERY_PARAMETER = "${SENSOR_ID}";

    private static final String DATETIME_QUERY_PARAMETER = "${DATETIME}";

    private static final String AGGREGATION_SCOPE_PARAMETER = "${AGGREGATION_SCOPE}";

    private static final String VALUE_QUERY_PARAMETER = "${VALUE}";

    private static final String CHECKPOINT_ATTRIBUTE_NAME = "checkpoint";

    private static final String GET_WITHOUT_DATASET_CHECKPOINT_QUERY = "SELECT MAX(records.DateTime) " + CHECKPOINT_ATTRIBUTE_NAME + " FROM " + DATASET_NAME_PARAMETER + ".SensorData records WHERE records.SensorId=\"" + SENSOR_ID_QUERY_PARAMETER + "\"";

    private static final String GET_WITHOUT_DATASET_AGGREGATED_CHECKPOINT_QUERY = "SELECT MAX(records.DateTime) " + CHECKPOINT_ATTRIBUTE_NAME + " FROM " + DATASET_NAME_PARAMETER + ".SensorDataAggregated records WHERE records.SensorId=\"" + SENSOR_ID_QUERY_PARAMETER + "\" AND AggregationScope=" + AGGREGATION_SCOPE_PARAMETER;

    private static final String GET_WITHOUT_DATASET_SENSOR_RAW_DATA_QUERY = "SELECT DateTime, Value FROM " + DATASET_NAME_PARAMETER + ".SensorData WHERE SensorId=\"" + SENSOR_ID_QUERY_PARAMETER + "\" AND DateTime>\"" + DATETIME_QUERY_PARAMETER + "\"";

    private static final String GET_WITHOUT_DATASET_SENSOR_IDS_QUERY = "SELECT SensorId FROM " + DATASET_NAME_PARAMETER + ".SensorNames";

    private static final String GET_WITHOUT_DATASET_HEATING_LEARNING_DATA = "SELECT DateTime, Value, SensorName, SensorId, Type, AggregationScope FROM " + DATASET_NAME_PARAMETER + ".HeatingLearningData  WHERE DateTime >= \"" + DATETIME_QUERY_PARAMETER + "\"";

    private static final String GET_WITHOUT_DATASET_HEATING_LEVEL_CHECKPOINT = "SELECT Max(DateTime) FROM " + DATASET_NAME_PARAMETER + ".HeatingLevel";

    // Used to calculate average during the last 30 minutes for heating thresholding
    private static final int MINUTES_BACK_TO_THE_PAST = 50;

    private BigQuery bigQuery;

    private final String dataSetName;

    public BigQueryConnector(String dataSetName) {
        this.dataSetName = dataSetName;
    }

    @Override
    public Optional<PlatformClientId> getPlatformId() {
        return Optional.of(PlatformClientId.BigQuery);
    }

    @Override
    public boolean begin(final PlatformCredentialsConfig.Credentials credentials, final File configDir) {
        if (this.bigQuery != null) {
            return true;
        }
        if (StringUtils.isEmpty(credentials.getKeyFilePath())) {
            throw new IllegalStateException("Cannot use BigQuery connector with key configuration file path configured");
        }
        final File keyFile = credentials.getKeyFile(configDir);
        if (!keyFile.exists()) {
            throw new IllegalStateException("BigQuery key file '" + keyFile.getPath() + "' does not exist");
        }

        GoogleCredentials googleCredentials;
        try (FileInputStream serviceAccountStream = new FileInputStream(keyFile)) {
            googleCredentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        } catch (FileNotFoundException e) {
            throw new IllegalStateException("BigQuery key file '" + keyFile.getPath() + "' does not exist");
        } catch (IOException e) {
            throw new IllegalStateException("BigQuery key file '" + keyFile.getPath() + "' cannot be read");
        }

        // Instantiate a client.
        this.bigQuery =
                BigQueryOptions.newBuilder().setCredentials(googleCredentials).build().getService();


        return true;
    }

    @Override
    public boolean addRecords(Iterable<SensorRecord> records, SensorNameProvider nameProvider) throws Exception {

        final TableId rawDataTableId = bigQuery.getTable(dataSetName, SENSOR_RECORDS_TABLE_NAME).getTableId();

        logger.info("Got table id '" + rawDataTableId + "'");

        int addedVakues = Iterables.size(records);



//        for (final Iterable<SensorRecord> page : Iterables.partition(Iterables.filter(records, incrementalFilter(records)), 10000)) {
        for (final Iterable<SensorRecord> page : Iterables.partition(records, 10000)) {
            final InsertAllRequest.Builder insertAllRequest = InsertAllRequest.newBuilder(rawDataTableId);

            for (final SensorRecord record : page) {
                if (record.getValue() != 0) {
                    insertAllRequest.addRow(ImmutableMap.<String, String>builder().
                            put("SensorId", record.getSensorUUID()).
                            put("DateTime", record.getDateTaken().toString()).
                            put("Date", record.getDateTaken().getYear() + "-" + record.getDateTaken().getMonthOfYear() + "-" + record.getDateTaken().getDayOfMonth()).
                            put("Time", record.getDateTaken().getHourOfDay() + ":" + record.getDateTaken().getMinuteOfHour() + ":" + record.getDateTaken().getSecondOfMinute()).
                            put("Value", formatFloat(record.getValue())).build());
                }
            }

            final InsertAllResponse insertAllResponse = bigQuery.insertAll(insertAllRequest.build());
            final Map<Long, List<BigQueryError>> errors = insertAllResponse.getInsertErrors();
            if (errors != null && errors.size() > 0) {
                for (final Long aLong : errors.keySet()) {
                    for (final BigQueryError error : errors.get(aLong)) {
                        logger.error("INSERT ERROR : '" + error.toString() + "'");
                    }
                }
                return false;
            }
        }
        logger.info("Added " + addedVakues + " elements to BigQuery");

        logger.info("Performing aggregations");

        final TableId aggregatedDataTableId = bigQuery.getTable(dataSetName, AGGREGATED_SENSOR_RECORDS_TABLE_NAME).getTableId();
        for (final String sensorId : getAllSensorIds()) {
            for (final AggregationScope scope : AggregationScope.values()) {
                logger.info("Performing '" + scope.name() + "' aggregation for sensor '" + sensorId + "'");
                final Iterable<AggregatedSensorRecord> aggregatedSensorRecords = getAggregatedValues(sensorId, scope);
                logger.info("Found " + Iterables.size(aggregatedSensorRecords) + " aggregated records for sensor '" + sensorId + "' on scope '" + scope.name() + "'");

                for (final Iterable<AggregatedSensorRecord> page : Iterables.partition(aggregatedSensorRecords, 10000)) {
                    final InsertAllRequest.Builder insertAllRequest = InsertAllRequest.newBuilder(aggregatedDataTableId);

                    for (final AggregatedSensorRecord aggregatedSensorRecord : page) {
                        insertAllRequest.addRow(ImmutableMap.<String, String>builder().
                                put("SensorId", sensorId).
                                put("DateTime", aggregatedSensorRecord.getPeriodEnd().toString()).
                                put("Date", aggregatedSensorRecord.getPeriodEnd().getYear() + "-" + aggregatedSensorRecord.getPeriodEnd().getMonthOfYear() + "-" + aggregatedSensorRecord.getPeriodEnd().getDayOfMonth()).
                                put("Value", formatFloat(aggregatedSensorRecord.getSensorValue(sensorId))).
                                put("AggregationScope", "" + scope.id()).build());
                    }

                    final InsertAllResponse insertAllResponse = bigQuery.insertAll(insertAllRequest.build());
                    final Map<Long, List<BigQueryError>> errors = insertAllResponse.getInsertErrors();
                    if (errors != null && errors.size() > 0) {
                        for (final Long aLong : errors.keySet()) {
                            for (final BigQueryError error : errors.get(aLong)) {
                                logger.error("INSERT ERROR : '" + error.toString() + "'");
                            }
                        }
                        return false;
                    }
                }
            }
        }

        final TableId heatingLevelTableId = bigQuery.getTable(dataSetName, HEATING_LEVEL_TABLE_NAME).getTableId();
        for (final Iterable<HeatingLevelRecord> heatingLevelRecords : Iterables.partition(getAggregatedHeatingLevelValues(), 10000)) {
            final InsertAllRequest.Builder insertAllRequest = InsertAllRequest.newBuilder(heatingLevelTableId);

            for (final HeatingLevelRecord heatingLevelRecord : heatingLevelRecords) {
                insertAllRequest.addRow(ImmutableMap.<String, String>builder().
                        put("DateTime", heatingLevelRecord.getDateTime().toString()).
                        put("HeatingLevel", formatFloat(heatingLevelRecord.getHeatingLevel())).
                        put("IndoorMonitorValue", formatFloat(heatingLevelRecord.getIndoorMonitorValue())).
                        put("OutdoorMonitorValue", formatFloat(heatingLevelRecord.getOutdoorMonitorValue())).
                        put("HeatingMonitorValue", formatFloat(heatingLevelRecord.getHeatingMonitorValue())).
                        put("PeriodType", heatingLevelRecord.getPerdiod().name()).build());
            }

            final InsertAllResponse insertAllResponse = bigQuery.insertAll(insertAllRequest.build());
            final Map<Long, List<BigQueryError>> errors = insertAllResponse.getInsertErrors();
            if (errors != null && errors.size() > 0) {
                for (final Long aLong : errors.keySet()) {
                    for (final BigQueryError error : errors.get(aLong)) {
                        logger.error("INSERT ERROR : '" + error.toString() + "'");
                    }
                }
                return false;
            }
        }


        return true;
    }

/*    private Predicate<SensorRecord> incrementalFilter(final Iterable<SensorRecord> records) {
        final Map<String, LocalDateTime> rawCheckPoints = Maps.newHashMap();
        for (final SensorRecord record : records) {
            if (!rawCheckPoints.containsKey(record.getSensorUUID())) {
                final Optional<LocalDateTime> checkPoint = getRawPointValue(record.getSensorUUID());
                if (checkPoint.isPresent()) {
                    rawCheckPoints.put(record.getSensorUUID(), checkPoint.get());
                } else {
                    rawCheckPoints.put(record.getSensorUUID(), LocalDateTime.parse("2019-11-5T00:00:00"));
                }
            }
        }
        return new Predicate<SensorRecord>() {
            @Override
            public boolean apply(@Nullable SensorRecord sensorRecord) {
                return sensorRecord.getDateTaken().isEqual(rawCheckPoints.get(sensorRecord.getSensorUUID()))
                        || sensorRecord.getDateTaken().isAfter(rawCheckPoints.get(sensorRecord.getSensorUUID()));
            }
        };
    }*/

    private String getQueryForDataSet(final String query) {
        return StringUtils.replace(query, DATASET_NAME_PARAMETER, dataSetName);
    }

    private Iterable<HeatingLevelRecord> getAggregatedHeatingLevelValues() {
        final LocalDateTime checkPointValue = getHeatingLevelCheckPoint();
        final String query = StringUtils.replace(getQueryForDataSet(GET_WITHOUT_DATASET_HEATING_LEARNING_DATA), DATETIME_QUERY_PARAMETER, checkPointValue.toString());

        final List<HeatingLevelRecord> result = Lists.newArrayList();

        logger.info("Running query '" + query + "'");
        final QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
        logger.debug("Iterating on query result '" + query + "'");
        try {
            for (FieldValueList row : bigQuery.query(queryConfig).iterateAll()) {
                logger.debug("Got ROW");
                // DateTime, Value, SensorName, SensorId, Type, AggregationScope
                final LocalDateTime dateTime;
                final float value;
                final String sensorName;
                final String sensorid;
                final SensorType type;

                if (!row.get(0).isNull()) {
                    dateTime = LocalDateTime.parse(row.get(0).getStringValue());
                } else {
                    dateTime = null;
                }
                if (!row.get(1).isNull()) {
                    value = new Double(row.get(1).getDoubleValue()).floatValue();
                } else {
                    value = -127;
                }
                if (!row.get(2).isNull()) {
                    sensorName = row.get(2).getStringValue();
                } else {
                    sensorName = null;
                }
                if (!row.get(3).isNull()) {
                    sensorid = row.get(3).getStringValue();
                } else {
                    sensorid = null;
                }
                if (!row.get(4).isNull()) {
                    type = SensorType.valueOf(row.get(4).getStringValue());
                } else {
                    type = null;
                }

                addTo(result, dateTime, type, value);
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException("Failed to run query '" + query + "'", e);
        }
        return ImmutableList.copyOf(Iterables.filter(result, DefaultHeatingLevelRecord.ReadyFilter()));
    }

    private boolean addTo(final List<HeatingLevelRecord> result, final LocalDateTime dateTime, final SensorType type, final float value) {
        if (type != SensorType.HEATING && type != SensorType.INDOOR && type != SensorType.OUTDOOR) {
            return false;
        }
        for (final HeatingLevelRecord heatingLevelRecord : result) {
            if (type == SensorType.HEATING) {
                if (heatingLevelRecord.addHeating(dateTime, value)) {
                    return true;
                }
            } else if (type == SensorType.INDOOR) {
                if (heatingLevelRecord.addIndoor(dateTime, value)) {
                    return true;
                }
            } else if (type == SensorType.OUTDOOR) {
                if (heatingLevelRecord.addOutdoor(dateTime, value)) {
                    return true;
                }
            }
        }
        final HeatingLevelRecord newValue = new DefaultHeatingLevelRecord(dateTime);
        result.add(newValue);
        return addTo(result, dateTime, type, value);
    }

    private Iterable<AggregatedSensorRecord> getAggregatedValues(final String sensorId, final AggregationScope scope) {
        final List<AggregatedSensorRecord> result = Lists.newArrayList();
        final LocalDateTime checkPoint = getAggregatedCheckPoint(sensorId, scope);
        final LocalDateTime now = LocalDateTime.now();

//        LocalDateTime windowStart = checkPoint;
//        LocalDateTime windowEnd = scope.nextValue(windowStart);

        Pair<LocalDateTime, LocalDateTime> window = Pair.with(scope.getStartDateTime(checkPoint), scope.nextValue(checkPoint));

        while (!scope.isEndOfWindow(window.getValue1())) {
            result.add(new DefaultAggregatedSensorRecord(window.getValue0(), window.getValue1(), new StaticSensorNameProvider()));
            window = scope.getNextPeriod(window.getValue1());
        }

        if (result.size() <= 0) {
            return Collections.emptyList();
        }

        for (final SensorRecord record : getRawDataForAggregation(sensorId, checkPoint.minusMinutes(MINUTES_BACK_TO_THE_PAST))) {
            for (final AggregatedSensorRecord aggregatedSensorRecord : result) {
                if (aggregatedSensorRecord.addValue(record)) {
                    break;
                }
            }
        }

        return ImmutableList.copyOf(Iterables.filter(result, new Predicate<AggregatedSensorRecord>() {

            @Override
            public boolean apply(@Nullable AggregatedSensorRecord aggregatedSensorRecord) {
                return aggregatedSensorRecord.hasSensorValue(sensorId);
            }

            @Override
            public boolean test(@Nullable AggregatedSensorRecord input) {
                return apply(input);
            }
        }));
    }

    private Iterable<SensorRecord> getRawDataForAggregation(final String sensorId, final LocalDateTime from) {
        final ImmutableList.Builder<SensorRecord> result = ImmutableList.builder();
        final String query = StringUtils.replaceEach(
                getQueryForDataSet(GET_WITHOUT_DATASET_SENSOR_RAW_DATA_QUERY),
                new String[] {SENSOR_ID_QUERY_PARAMETER, DATETIME_QUERY_PARAMETER},
                new String[] {sensorId, from.toString()});
        logger.info("Running query '" + query + "'");
        final QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
        logger.debug("Iterating on query result '" + query + "'");
        try {
            for (FieldValueList row : bigQuery.query(queryConfig).iterateAll()) {
                logger.debug("Got ROW");
                if (!row.get(0).isNull() && !row.get(1).isNull()) {
                    result.add(new TemperatureRecord(new LocalDateTime(row.get(0).getStringValue()), new Float(row.get(1).getDoubleValue()), sensorId));
                }
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException("Failed to run query '" + query + "'", e);
        }
        return result.build();
    }

    private LocalDateTime getAggregatedCheckPoint(final String sensorId, final AggregationScope aggregationScope) {
        final String query = StringUtils.replaceEach(
                getQueryForDataSet(GET_WITHOUT_DATASET_AGGREGATED_CHECKPOINT_QUERY),
                new String[] {SENSOR_ID_QUERY_PARAMETER, AGGREGATION_SCOPE_PARAMETER},
                new String[] {sensorId, "" + aggregationScope.id()});
        logger.debug("Running query '" + query + "'");
        final QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
        logger.debug("Iterating on query result '" + query + "'");
        try {
            for (FieldValueList row : bigQuery.query(queryConfig).iterateAll()) {
                logger.debug("Got ROW");
                for (FieldValue val : row) {
                    logger.debug("Got attribute '" + val.toString() + "'");
                    if (!val.isNull()) {
                        logger.info("Checkpoint value for sensor '" + sensorId + "' is '" + val.getStringValue() + "'");
                        return new LocalDateTime(val.getStringValue());
                    }
                }
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException("Failed to run query '" + query + "'", e);
        }
        return new LocalDateTime("2019-11-04T00:00:00");
    }

    private LocalDateTime getHeatingLevelCheckPoint() {
        final String query = getQueryForDataSet(GET_WITHOUT_DATASET_HEATING_LEVEL_CHECKPOINT);
        logger.debug("Running query '" + query + "'");
        final QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
        logger.debug("Iterating on query result '" + query + "'");
        try {
            for (FieldValueList row : bigQuery.query(queryConfig).iterateAll()) {
                logger.debug("Got ROW");
                for (FieldValue val : row) {
                    logger.debug("Got attribute '" + val.toString() + "'");
                    if (!val.isNull()) {
                        logger.info("Heating level checkpoint value is '" + val.getStringValue() + "'");
                        return new LocalDateTime(val.getStringValue());
                    }
                }
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException("Failed to run query '" + query + "'", e);
        }
        return DEFAULT_CHECKPOINT_VALUE;
    }

    private Iterable<String> getAllSensorIds() {
        final ImmutableList.Builder<String> result = ImmutableList.builder();
        final String query = getQueryForDataSet(GET_WITHOUT_DATASET_SENSOR_IDS_QUERY);
        logger.debug("Running query '" + query + "'");
        final QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
        logger.debug("Iterating on query result '" + query + "'");
        try {
            for (FieldValueList row : bigQuery.query(queryConfig).iterateAll()) {
                logger.debug("Got ROW");
                for (FieldValue val : row) {
                    logger.debug("Got attribute '" + val.toString() + "'");
                    if (!val.isNull()) {
                        result.add(val.getStringValue());
                    }
                }
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException("Failed to run query '" + query + "'", e);
        }
        return result.build();
    }

    private static String formatFloat(final float value) {
        DecimalFormat df = new DecimalFormat("#.#");
        return df.format(new Float(value).doubleValue());
    }

    public Optional<LocalDateTime> getRawCheckpointValue(String sensorId) {
        final String query = StringUtils.replace(getQueryForDataSet(GET_WITHOUT_DATASET_CHECKPOINT_QUERY), SENSOR_ID_QUERY_PARAMETER, sensorId);
        logger.debug("Running query '" + query + "'");
        final QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
        logger.debug("Iterating on query result '" + query + "'");
        try {
            for (FieldValueList row : bigQuery.query(queryConfig).iterateAll()) {
                logger.debug("Got ROW");
                for (FieldValue val : row) {
                    logger.debug("Got attribute '" + val.toString() + "'");
                    if (!val.isNull()) {
                        logger.info("Checkpoint value for sensor '" + sensorId + "' is '" + val.getStringValue() + "'");
                        return Optional.of(new LocalDateTime(val.getStringValue()));
                    }
                }
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException("Failed to run query '" + query + "'", e);
        }
        return Optional.absent();
    }

    @Override
    public Optional<LocalDateTime> getCheckPointValue(String sensorId) {
        return getRawCheckpointValue(sensorId);
    }

    @Override
    public boolean end() {
        return true;
    }
}
