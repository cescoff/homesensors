package com.desi.data.csv;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.desi.data.DataSource;
import com.desi.data.PlatformClientId;
import com.desi.data.SensorNameProvider;
import com.desi.data.SensorRecord;
import com.desi.data.config.PlatformCredentialsConfig;
import com.desi.data.utils.TemperatureCSVParser;
import com.google.common.base.Optional;
import com.google.common.collect.*;
import org.apache.commons.lang.StringUtils;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class S3CSVDataSource implements DataSource {

    private static Logger logger = LoggerFactory.getLogger(S3CSVDataSource.class);

    private static final String BUCKET_NAME = "desi-sensors";

    private String accessKey;

    private String secretKey;

    private AtomicBoolean INIT_DONE = new AtomicBoolean(false);

    private AmazonS3 s3 = null;

    private final SensorNameProvider sensorNameProvider;

    private Set<String> uuids = Sets.newHashSet();

    private Iterable<SensorRecord> sensorRecords = Lists.newArrayList();

    private final String folder;

    public S3CSVDataSource(SensorNameProvider sensorNameProvider, String folder) {
        this.sensorNameProvider = sensorNameProvider;
        this.folder = folder;
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

            s3 = AmazonS3ClientBuilder.standard().
                    withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey))).
                    withRegion(Regions.EU_WEST_3).
                    build();

            final ListObjectsV2Result result = s3.listObjectsV2(BUCKET_NAME);

            final List<S3ObjectSummary> objects = result.getObjectSummaries();

            final ImmutableList.Builder<SensorRecord> records = ImmutableList.builder();

            for (S3ObjectSummary os : objects) {
                try {
                    if (StringUtils.contains(os.getKey(), folder + "/") && !StringUtils.contains(os.getKey(), "archives/")) {
                        S3Object fullObject = s3.getObject(new GetObjectRequest(os.getBucketName(), os.getKey()));
                        logger.info("Parsing content for object s3://" + os.getBucketName() + "/" + os.getKey());
                        records.addAll(TemperatureCSVParser.parseContent(fullObject.getObjectContent()));
                    }
                } catch (Exception e) {
                    logger.error("Failed to parse file s3://" + os.getBucketName() + "/" + os.getKey() + ": " + e.getMessage(), e);
                }
            }
            this.sensorRecords = records.build();
            return true;
        }
        throw new IllegalStateException("No service '" + PlatformClientId.S3Bridge + "' configured into configuration file");
    }

    @Override
    public Iterable<String> getSensorsUUIDs() {
        ImmutableSet.Builder<String> uuids = ImmutableSet.builder();
        for (final SensorRecord record : sensorRecords) uuids.add(record.getSensorUUID());
        return uuids.build();
    }

    @Override
    public Map<String, SensorRecord> getLastSensorsValues() {
        return ImmutableMap.<String, SensorRecord>builder().build();
    }

    @Override
    public Iterable<SensorRecord> getRecords(String uuid, LocalDateTime checkPoint) {
        final ImmutableList.Builder<SensorRecord> result = ImmutableList.builder();
        for (final SensorRecord sensorRecord : sensorRecords) {
            if (uuid.equals(sensorRecord.getSensorUUID()) && sensorRecord.getDateTaken().isAfter(checkPoint)) {
                result.add(sensorRecord);
            }
        }
        return result.build();
    }

    @Override
    public boolean end() {
        s3.shutdown();
        return true;
    }
}
