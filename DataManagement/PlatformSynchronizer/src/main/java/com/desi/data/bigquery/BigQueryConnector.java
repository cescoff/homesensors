package com.desi.data.bigquery;

import com.desi.data.*;
import com.desi.data.bean.DefaultAggregatedSensorRecord;
import com.desi.data.bean.TemperatureRecord;
import com.desi.data.config.PlatformCredentialsConfig;
import com.desi.data.impl.StaticSensorNameProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;

public class BigQueryConnector implements Connector {

    private static final Logger logger = LoggerFactory.getLogger(BigQueryConnector.class);

    private static final String DATASET_NAME = "Records";

    private static final String SENSOR_RECORDS_TABLE_NAME = "SensorData";

    private static final String AGGREGATED_SENSOR_RECORDS_TABLE_NAME = "SensorDataAggregated";

    private static final String SENSOR_ID_QUERY_PARAMETER = "${SENSOR_ID}";

    private static final String DATETIME_QUERY_PARAMETER = "${DATETIME}";

    private static final String AGGREGATION_SCOPE_PARAMETER = "${AGGREGATION_SCOPE}";

    private static final String VALUE_QUERY_PARAMETER = "${VALUE}";

    private static final String CHECKPOINT_ATTRIBUTE_NAME = "checkpoint";

    private static final String GET_CHECKPOINT_QUERY = "SELECT MAX(records.DateTime) " + CHECKPOINT_ATTRIBUTE_NAME + " FROM Records.SensorData records WHERE records.SensorId=\"" + SENSOR_ID_QUERY_PARAMETER + "\"";

    private static final String GET_AGGREGATED_CHECKPOINT_QUERY = "SELECT MAX(records.DateTime) " + CHECKPOINT_ATTRIBUTE_NAME + " FROM Records.SensorDataAggregated records WHERE records.SensorId=\"" + SENSOR_ID_QUERY_PARAMETER + "\" AND AggregationScope=" + AGGREGATION_SCOPE_PARAMETER;

    private static final String GET_SENSOR_RAW_DATA_QUERY = "SELECT DateTime, Value FROM Records.SensorData WHERE SensorId=\"" + SENSOR_ID_QUERY_PARAMETER + "\" AND DateTime>\"" + DATETIME_QUERY_PARAMETER + "\"";

    private static final String GET_SENSOR_IDS_QUERY = "SELECT SensorId FROM Records.SensorNames";


    private BigQuery bigQuery;


    @Override
    public Optional<PlatformClientId> getPlatformId() {
        return Optional.of(PlatformClientId.BigQuery);
    }

    @Override
    public boolean begin(final PlatformCredentialsConfig.Credentials credentials) {
        if (this.bigQuery != null) {
            return true;
        }
        if (StringUtils.isEmpty(credentials.getKeyFilePath())) {
            throw new IllegalStateException("Cannot use BigQuery connector with key configuration file path configured");
        }
        final File keyFile = new File(credentials.getKeyFilePath());
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

        final TableId rawDataTableId = bigQuery.getTable(DATASET_NAME,SENSOR_RECORDS_TABLE_NAME).getTableId();

        logger.info("Got table id '" + rawDataTableId + "'");

        int addedVakues = Iterables.size(records);

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

        final TableId aggregatedDataTableId = bigQuery.getTable(DATASET_NAME,AGGREGATED_SENSOR_RECORDS_TABLE_NAME).getTableId();
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

        return true;
    }

    private Iterable<AggregatedSensorRecord> getAggregatedValues(final String sensorId, final AggregationScope scope) {
        final List<AggregatedSensorRecord> result = Lists.newArrayList();
        final LocalDateTime checkPoint = getAggregatedCheckPoint(sensorId, scope);
        final LocalDateTime now = LocalDateTime.now();

        LocalDateTime windowStart = checkPoint;
        LocalDateTime windowEnd = scope.nextValue(windowStart);

        while (!scope.isEndOfWindow(windowEnd)) {
            result.add(new DefaultAggregatedSensorRecord(windowStart, windowEnd, new StaticSensorNameProvider()));
            windowStart = windowEnd;
            windowEnd = scope.nextValue(windowEnd);
        }

        for (final SensorRecord record : getRawDataForAggregation(sensorId, checkPoint)) {
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
                GET_SENSOR_RAW_DATA_QUERY,
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
                GET_AGGREGATED_CHECKPOINT_QUERY,
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

    private Iterable<String> getAllSensorIds() {
        final ImmutableList.Builder<String> result = ImmutableList.builder();
        final String query = GET_SENSOR_IDS_QUERY;
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

    @Override
    public Optional<LocalDateTime> getCheckPointValue(String sensorId) {
        final String query = StringUtils.replace(GET_CHECKPOINT_QUERY, SENSOR_ID_QUERY_PARAMETER, sensorId);
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
    public boolean end() {
        return true;
    }
}
