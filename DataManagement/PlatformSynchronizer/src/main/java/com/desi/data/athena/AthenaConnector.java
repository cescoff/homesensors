package com.desi.data.athena;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.athena.model.*;
import com.desi.data.DataSource;
import com.desi.data.PlatformClientId;
import com.desi.data.SensorNameProvider;
import com.desi.data.SensorRecord;
import com.desi.data.bean.TemperatureRecord;
import com.desi.data.config.PlatformCredentialsConfig;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.commons.lang.StringUtils;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class AthenaConnector implements DataSource {

    private static final Logger logger = LoggerFactory.getLogger(AthenaConnector.class);

    private static final String DATABASE_NAME = "desisensors";

    private static final String EXTERNAL_UUIDS = "SELECT DISTINCT(uuid) FROM desisensors.external_temperatures";

    private static final String INTERNAL_UUIDS = "SELECT DISTINCT(uuid) FROM desisensors.internal_temperatures";

    private static final String UUID_PARAMETER = "${uuid}";

    private static final String ORDERING_PARAMETER = "${ordering}";

    private static final String INTERNAL_TEMPERATURES_QUERY = "SELECT date_parse(concat(date, 'T', time), '%Y-%m-%dT%H:%i:%s') datetime, uuid, replace(temperature_c, 'C=', '') temperature FROM desisensors.internal_temperatures WHERE uuid='" + UUID_PARAMETER + "' ORDER BY datetime " + ORDERING_PARAMETER;

    private static final String EXTERNAL_TEMPERATURES_QUERY = "SELECT date_parse(concat(date, 'T', time), '%d/%m/%YT%H:%i:%s') datetime, uuid, replace(temperature_c, 'C=', '') temperature FROM desisensors.external_temperatures WHERE uuid='" + UUID_PARAMETER + "' ORDER BY datetime " + ORDERING_PARAMETER;

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

    private String accessKey;

    private String secretKey;

    private AtomicBoolean INIT_DONE = new AtomicBoolean(false);

    private AmazonAthena athena = null;

    private final SensorNameProvider sensorNameProvider;

    public AthenaConnector(SensorNameProvider sensorNameProvider) {
        this.sensorNameProvider = sensorNameProvider;
    }

    @Override
    public Optional<PlatformClientId> getPlatformId() {
        return Optional.of(PlatformClientId.S3Bridge);
    }

    @Override
    public boolean begin(PlatformCredentialsConfig.Credentials credentials, File configDir) {
        if (INIT_DONE.get()) {
            return true;
        }

        if (credentials.getId() == PlatformClientId.S3Bridge) {
            this.accessKey = credentials.getAccessKey();
            this.secretKey = credentials.getSecretKey();
            this.INIT_DONE.set(true);

            this.athena = AmazonAthenaClientBuilder.standard()
                    .withRegion(Regions.EU_WEST_3)
                    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                    .withClientConfiguration(new ClientConfiguration().withClientExecutionTimeout(100000)).build();

            return true;
        }
        throw new IllegalStateException("No service '" + PlatformClientId.S3Bridge + "' configured into configuration file");
    }

    @Override
    public Iterable<String> getSensorsUUIDs() {
        final ImmutableSet.Builder<String> result = ImmutableSet.builder();

        for (final String query : ImmutableList.<String>of(INTERNAL_UUIDS, EXTERNAL_UUIDS)) {
            final String queryExecutionId = submitAthenaQuery(athena, query);
            try {
                waitForQueryToComplete(athena, queryExecutionId, "GetSensorsUUIDs");
            } catch (InterruptedException e) {
                logger.error("Failed to wait for completion on query '" + query + "'");
                throw new IllegalStateException("Cannot list uuids on query '" + query + "'", e);
            }

            result.addAll(processUUIDRows(athena, queryExecutionId));
        }

        return result.build();
    }

    @Override
    public Map<String, SensorRecord> getLastSensorsValues() {
        final ImmutableMap.Builder<String, SensorRecord> result = ImmutableMap.builder();

        for (final String uuid : getSensorsUUIDs()) {
            final Iterable<SensorRecord> lastValues = getRecords(uuid, LocalDateTime.now().minusHours(6), "DESC");
            if (!Iterables.isEmpty(lastValues)) {
                result.put(uuid, Iterables.getFirst(lastValues, null));
            }
        }

        return result.build();
    }

    @Override
    public Iterable<SensorRecord> getRecords(String uuid, LocalDateTime checkPoint) {
        return getRecords(uuid, checkPoint, "ASC");
    }

    private Iterable<SensorRecord> getRecords(String uuid, LocalDateTime checkPoint, final String ordering) {
        final ImmutableSet.Builder<SensorRecord> result = ImmutableSet.builder();

        for (final String query : ImmutableList.<String>of(INTERNAL_TEMPERATURES_QUERY, EXTERNAL_TEMPERATURES_QUERY)) {
            final String queryExecutionId = submitAthenaQuery(athena, contextualizeQuery(query, uuid, ordering));
            try {
                waitForQueryToComplete(athena, queryExecutionId, "GetRecords");
            } catch (InterruptedException e) {
                logger.error("Failed to wait for completion on query '" + query + "'");
                throw new IllegalStateException("Cannot list uuids on query '" + query + "'", e);
            }

            result.addAll(parseSensors(athena, queryExecutionId, uuid, checkPoint));
        }

        return result.build();
    }

    private String contextualizeQuery(final String query, final String uuid, final String ordering) {
        String result = query;
        result = StringUtils.replace(result, UUID_PARAMETER, uuid);
        result = StringUtils.replace(result, ORDERING_PARAMETER, ordering);
        return result;
    }

    @Override
    public boolean end() {
        athena.shutdown();
        return true;
    }

    private static String submitAthenaQuery(final AmazonAthena athenaClient, final String sql) {
        // The QueryExecutionContext allows us to set the Database.
        QueryExecutionContext queryExecutionContext = new QueryExecutionContext().withDatabase(DATABASE_NAME);

        // The result configuration specifies where the results of the query should go in S3 and encryption options
        ResultConfiguration resultConfiguration = new ResultConfiguration()
                // You can provide encryption options for the output that is written.
                // .withEncryptionConfiguration(encryptionConfiguration)
                .withOutputLocation("s3://aws-athena-query-results-066100256781-eu-west-3");

        // Create the StartQueryExecutionRequest to send to Athena which will start the query.
        StartQueryExecutionRequest startQueryExecutionRequest = new StartQueryExecutionRequest()
                .withQueryString(sql)
                .withQueryExecutionContext(queryExecutionContext)
                .withResultConfiguration(resultConfiguration);

        StartQueryExecutionResult startQueryExecutionResult = athenaClient.startQueryExecution(startQueryExecutionRequest);
        return startQueryExecutionResult.getQueryExecutionId();
    }

    private void waitForQueryToComplete(final AmazonAthena athenaClient, final String queryExecutionId, final String queryName) throws InterruptedException
    {
        GetQueryExecutionRequest getQueryExecutionRequest = new GetQueryExecutionRequest()
                .withQueryExecutionId(queryExecutionId);

        GetQueryExecutionResult getQueryExecutionResult = null;
        boolean isQueryStillRunning = true;
        while (isQueryStillRunning) {
            getQueryExecutionResult = athenaClient.getQueryExecution(getQueryExecutionRequest);
            String queryState = getQueryExecutionResult.getQueryExecution().getStatus().getState();
            if (queryState.equals(QueryExecutionState.FAILED.toString())) {
                throw new RuntimeException("Query Failed to run with Error Message: " + getQueryExecutionResult.getQueryExecution().getStatus().getStateChangeReason());
            }
            else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
                throw new RuntimeException("Query was cancelled.");
            }
            else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
                isQueryStillRunning = false;
            }
            else {
                // Sleep an amount of time before retrying again.
                Thread.sleep(500);
            }
            logger.info("Query '" + queryName + "' Status is: " + queryState);
        }
    }

    /**
     * This code calls Athena and retrieves the results of a query.
     * The query must be in a completed state before the results can be retrieved and
     * paginated. The first row of results are the column headers.
     */
    private Iterable<String> processUUIDRows(AmazonAthena athenaClient, String queryExecutionId) {
        GetQueryResultsRequest getQueryResultsRequest = new GetQueryResultsRequest()
                // Max Results can be set but if its not set,
                // it will choose the maximum page size
                // As of the writing of this code, the maximum value is 1000
                // .withMaxResults(1000)
                .withQueryExecutionId(queryExecutionId);

        GetQueryResultsResult getQueryResultsResult = athenaClient.getQueryResults(getQueryResultsRequest);
        List<ColumnInfo> columnInfoList = getQueryResultsResult.getResultSet().getResultSetMetadata().getColumnInfo();

        ImmutableSet.Builder<String> uuids = ImmutableSet.builder();
        while (true) {
            List<Row> results = getQueryResultsResult.getResultSet().getRows();
            for (Row row : results) {
                logger.debug("Row::'" + row.getData().get(0).getVarCharValue() + "'");
                final String value = row.getData().get(0).getVarCharValue();
                if (!"uuid".equals(value)) {
                    uuids.add(value);
                }
                // Process the row. The first row of the first page holds the column names.
            }
            // If nextToken is null, there are no more pages to read. Break out of the loop.
            if (getQueryResultsResult.getNextToken() == null) {
                break;
            }
            getQueryResultsResult = athenaClient.getQueryResults(
                    getQueryResultsRequest.withNextToken(getQueryResultsResult.getNextToken()));
        }
        return uuids.build();
    }

    private Iterable<SensorRecord> parseSensors(final AmazonAthena athenaClient, final String queryExecutionId, final String uuid, final LocalDateTime checkPoint) {
        logger.info("Parsing 'GetRecords' query result");
        GetQueryResultsRequest getQueryResultsRequest = new GetQueryResultsRequest()
                // Max Results can be set but if its not set,
                // it will choose the maximum page size
                // As of the writing of this code, the maximum value is 1000
                // .withMaxResults(1000)
                .withQueryExecutionId(queryExecutionId);

        GetQueryResultsResult getQueryResultsResult = athenaClient.getQueryResults(getQueryResultsRequest);
        List<ColumnInfo> columnInfoList = getQueryResultsResult.getResultSet().getResultSetMetadata().getColumnInfo();

        int count = 0;
        ImmutableList.Builder<SensorRecord> records = ImmutableList.builder();
        while (true) {
            List<Row> results = getQueryResultsResult.getResultSet().getRows();
            for (Row row : results) {
                final String sensorUUID = row.getData().get(1).getVarCharValue();
                logger.debug("UUID::'" + row.getData().get(0).getVarCharValue() + "'");
                if (uuid.equals(sensorUUID)) {
                    final String dateTimeString = StringUtils.remove(row.getData().get(0).getVarCharValue(), ".000");
                    final Float temperature = Float.parseFloat(row.getData().get(2).getVarCharValue());

                    final LocalDateTime recordDateTime = DATE_TIME_FORMATTER.parseLocalDateTime(dateTimeString);
                    if (recordDateTime.isAfter(checkPoint)) {
                        records.add(new TemperatureRecord(recordDateTime, temperature, uuid, sensorNameProvider.getType(uuid)));
                    }
                }
                count++;
                if (count % 10000 == 0) {
                    logger.info("On query 'GetRecords' " + count + " rows have been explored");
                }
                // Process the row. The first row of the first page holds the column names.
            }
            // If nextToken is null, there are no more pages to read. Break out of the loop.
            if (getQueryResultsResult.getNextToken() == null) {
                break;
            }
            getQueryResultsResult = athenaClient.getQueryResults(
                    getQueryResultsRequest.withNextToken(getQueryResultsResult.getNextToken()));
        }
        logger.info("Done parsing 'GetRecords' query result");
        return records.build();
    }

    private static void processRow(Row row, List<ColumnInfo> columnInfoList)
    {
        for (int i = 0; i < columnInfoList.size(); ++i) {
            switch (columnInfoList.get(i).getType()) {
                case "varchar":
                    // Convert and Process as String
                    break;
                case "tinyint":
                    // Convert and Process as tinyint
                    break;
                case "smallint":
                    // Convert and Process as smallint
                    break;
                case "integer":
                    // Convert and Process as integer
                    break;
                case "bigint":
                    // Convert and Process as bigint
                    break;
                case "double":
                    // Convert and Process as double
                    break;
                case "boolean":
                    // Convert and Process as boolean
                    break;
                case "date":
                    // Convert and Process as date
                    break;
                case "timestamp":
                    // Convert and Process as timestamp
                    break;
                default:
                    throw new RuntimeException("Unexpected Type is not expected" + columnInfoList.get(i).getType());
            }
        }
    }
}
