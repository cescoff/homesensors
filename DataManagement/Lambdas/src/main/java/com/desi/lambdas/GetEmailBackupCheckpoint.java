package com.desi.lambdas;
// com.desi.lambdas.GetEmailBackupCheckpoint
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.model.*;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.desi.athena.AthenaClientFactory;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;

public class GetEmailBackupCheckpoint implements RequestHandler<Void, String> {

    // 2019-06-25 13:00:36.000
    private static final DateTimeFormatter INPUT_DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final DateTimeFormatter OUTPUT_DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    public static void main(String[] args) {
        new GetEmailBackupCheckpoint().handleRequest(null, null);
    }

    @Override
    public String handleRequest(Void s, Context context) {

        final AmazonAthena athenaClient = new AthenaClientFactory().createClient();
        final String queryExecutionId = submitAthenaQuery(athenaClient);
        try {
            waitForQueryToComplete(athenaClient, queryExecutionId);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return "ERROR";
        }

        final String value = processResultRows(context, athenaClient, queryExecutionId);

        if (value != null) {
            final LocalDateTime checkPointDate = INPUT_DATE_FORMAT.parseLocalDateTime(value);
            return OUTPUT_DATE_FORMAT.print(checkPointDate) + "Z";
        }

        return OUTPUT_DATE_FORMAT.print(LocalDateTime.now().withDayOfMonth(1).withMonthOfYear(1).withYear(2018).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0)) + "Z";
    }

    private static String submitAthenaQuery(AmazonAthena athenaClient) {
        // The QueryExecutionContext allows us to set the Database.
        QueryExecutionContext queryExecutionContext = new QueryExecutionContext().withDatabase("evernexemailarchives");

        // The result configuration specifies where the results of the query should go in S3 and encryption options
        ResultConfiguration resultConfiguration = new ResultConfiguration()
                // You can provide encryption options for the output that is written.
                // .withEncryptionConfiguration(encryptionConfiguration)
                .withOutputLocation("s3://aws-athena-query-results-066100256781-eu-west-3");

        // Create the StartQueryExecutionRequest to send to Athena which will start the query.
        StartQueryExecutionRequest startQueryExecutionRequest = new StartQueryExecutionRequest()
                .withQueryString("SELECT MAX(date_parse(datetime, '%Y-%m-%dT%H:%i:%s.000Z')) FROM emailcheckpoints")
                .withQueryExecutionContext(queryExecutionContext)
                .withResultConfiguration(resultConfiguration);

        StartQueryExecutionResult startQueryExecutionResult = athenaClient.startQueryExecution(startQueryExecutionRequest);
        return startQueryExecutionResult.getQueryExecutionId();
    }

    private void waitForQueryToComplete(AmazonAthena athenaClient, String queryExecutionId) throws InterruptedException
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
            System.out.println("Current Status is: " + queryState);
        }
    }

    /**
     * This code calls Athena and retrieves the results of a query.
     * The query must be in a completed state before the results can be retrieved and
     * paginated. The first row of results are the column headers.
     */
    private String processResultRows(Context context, AmazonAthena athenaClient, String queryExecutionId) {
        GetQueryResultsRequest getQueryResultsRequest = new GetQueryResultsRequest()
                // Max Results can be set but if its not set,
                // it will choose the maximum page size
                // As of the writing of this code, the maximum value is 1000
                // .withMaxResults(1000)
                .withQueryExecutionId(queryExecutionId);

        GetQueryResultsResult getQueryResultsResult = athenaClient.getQueryResults(getQueryResultsRequest);
        List<ColumnInfo> columnInfoList = getQueryResultsResult.getResultSet().getResultSetMetadata().getColumnInfo();

        String lastValue = null;
        while (true) {
            List<Row> results = getQueryResultsResult.getResultSet().getRows();
            for (Row row : results) {
                context.getLogger().log("Row::'" + row.getData().get(0).getVarCharValue() + "'");
                lastValue = row.getData().get(0).getVarCharValue();
                // Process the row. The first row of the first page holds the column names.
            }
            // If nextToken is null, there are no more pages to read. Break out of the loop.
            if (getQueryResultsResult.getNextToken() == null) {
                break;
            }
            getQueryResultsResult = athenaClient.getQueryResults(
                    getQueryResultsRequest.withNextToken(getQueryResultsResult.getNextToken()));
        }
        return lastValue;
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
