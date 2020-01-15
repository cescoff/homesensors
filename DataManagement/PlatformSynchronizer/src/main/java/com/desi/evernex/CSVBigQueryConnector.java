package com.desi.evernex;

import com.desi.data.*;
import com.desi.data.bean.*;
import com.desi.data.bigquery.AggregationScope;
import com.desi.data.bigquery.BigQueryConnector;
import com.desi.data.config.PlatformCredentialsConfig;
import com.desi.data.impl.StaticSensorNameProvider;
import com.desi.data.utils.JAXBUtils;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.PropertyConfigurator;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.javatuples.Pair;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class CSVBigQueryConnector {

    private static final Logger logger = LoggerFactory.getLogger(CSVBigQueryConnector.class);

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forPattern("dd/MM/yyyy");
    private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm");

    private static final String DATASET_NAME_PARAMETER = "${DATASET}";

    private final File awsCredentialsConfigurationFile;

    private final PlatformClientId clientId;

    private PlatformCredentialsConfig credentialsConfig;

    private AtomicBoolean INIT_DONE = new AtomicBoolean(false);

    public CSVBigQueryConnector(File awsCredentialsConfigurationFile, PlatformClientId clientId, String dataSetName) {
        this.awsCredentialsConfigurationFile = awsCredentialsConfigurationFile;
        this.clientId = clientId;
        this.dataSetName = dataSetName;
    }

    // "Last Activity";"Account Owner";"Company Name";"Billing State/Province (text only)";"Last Modified Date";"Id_SAP";"Created Date";"Last Opportunity date";"Account ID";"Is_SAP"
    private static final Map<String, String> MAPPINGS = ImmutableMap.<String, String>builder().
            put("Last Activity", "LastActivity").
            put("Account Owner", "AccountOwner").
            put("Company Name", "CompanyName").
            put("Billing State/Province (text only)", "BillingState").
            put("Last Modified Date", "LastModified").
            put("Id_SAP", "Id_SAP").
            put("Created Date", "CreatedDate").
            put("Last Opportunity date", "LastOpportunityDate").
            put("Account ID", "AccountID").build();

    // Used to calculate average during the last 30 minutes for heating thresholding
    private static final int MINUTES_BACK_TO_THE_PAST = 50;

    private BigQuery bigQuery;

    private final String dataSetName;

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

    public boolean addRecords(final File csvFile, final String tableName) throws Exception {
        init();
        CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_8, CSVFormat.EXCEL.withHeader().withDelimiter(';'));

        for (final Table table : bigQuery.listTables(dataSetName).getValues()) {
            logger.info("Table '" + table.getTableId() + "'");
        }

        final TableId rawDataTableId = bigQuery.getTable(dataSetName, tableName).getTableId();

        logger.info("Got table id '" + rawDataTableId + "'");


//        for (final Iterable<SensorRecord> page : Iterables.partition(Iterables.filter(records, incrementalFilter(records)), 10000)) {
        for (final Iterable<CSVRecord> page : Iterables.partition(parser.getRecords(), 10000)) {
            final InsertAllRequest.Builder insertAllRequest = InsertAllRequest.newBuilder(rawDataTableId);

            for (final CSVRecord csvRecord : page) {
                final ImmutableMap.Builder<String, String> row = ImmutableMap.builder();
                for (final String fromName : MAPPINGS.keySet()) {
                    final String toName = MAPPINGS.get(fromName);
                    if (StringUtils.isNotEmpty(csvRecord.get(fromName))) {
                        String parsedValue = null;
                        try {
                            parsedValue = DATE_TIME_FORMAT.parseLocalDateTime(csvRecord.get(fromName)).toString();
                        } catch (Throwable t) {
                        }
                        if (parsedValue == null) {
                            try {
                                parsedValue = DATE_FORMAT.parseLocalDateTime(csvRecord.get(fromName)).toString();
                            } catch (Throwable t) {
                                parsedValue = csvRecord.get(fromName);
                            }
                        }
                        row.put(toName, parsedValue);
                    }
                }
                insertAllRequest.addRow(row.build());
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
        logger.info("Added " + parser.getRecordNumber() + " entries to BigQuery");
        return true;
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
                begin(credentials, awsCredentialsConfigurationFile.getParentFile());
                this.INIT_DONE.set(true);
                return;
            }
        }
        throw new IllegalStateException("No service '" + this.clientId + "' configured into file '" + this.awsCredentialsConfigurationFile.getPath() + "'");
    }


    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage " + S3Bridge.class.getSimpleName() + " <CREDENTIALS_FILE_PATH> <CSV_FILE>");
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
        try {
/*            if (!new S3Bridge(ImmutableList.<Connector>of(new SpreadSheetConverter()), new File(args[0]), PlatformClientId.S3Bridge).sync()) {
                logger.warn("Synchronization process returned any data synchronized");
                System.exit(4);
            }*/
            if (!new CSVBigQueryConnector(new File(args[0]), PlatformClientId.BigQuery, "Evernex").addRecords(new File(args[1]), "Accounts")) {
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
