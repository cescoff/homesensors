package com.desi.server.servlet;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.desi.data.PlatformClientId;
import com.desi.data.SensorRecord;
import com.desi.data.config.PlatformCredentialsConfig;
import com.desi.data.utils.JAXBUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ImageS3Servlet extends HttpServlet {

    private static final Logger logger = LoggerFactory.getLogger(ImageS3Servlet.class);

//    private static final Map<String, String> BUCKET_REGION_MAPPINGS =

//    private final File awsCredentialsConfigurationFile;

    private PlatformCredentialsConfig credentialsConfig;

    private String accessKey;

    private String secretKey;

    private AtomicBoolean INIT_DONE = new AtomicBoolean(false);

    private LocalDateTime lastRefresh = null;

    private final Map<String, String> s3Mappings = Maps.newHashMap();
/*
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        initS3();
        super.doGet(req, resp);
    }

    private void refreshCache() {
        if (lastRefresh != null && lastRefresh.isAfter(LocalDateTime.now().minusHours(2))) {
            return;
        }
        synchronized (s3Mappings) {
            if (lastRefresh != null && lastRefresh.isAfter(LocalDateTime.now().minusHours(2))) {
                return;
            }
            final AmazonS3 s3 = AmazonS3ClientBuilder.standard().
                    withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey))).
                    withRegion(REGION).
                    build();
            final ListObjectsV2Result result = s3.listObjectsV2(BUCKET_NAME);

            final List<S3ObjectSummary> objects = result.getObjectSummaries();

            final ImmutableList.Builder<SensorRecord> records = ImmutableList.builder();

            for (S3ObjectSummary os : objects) {
                try {
                    if (StringUtils.contains(os.getKey(), folder + "/") && !StringUtils.contains(os.getKey(), "archives/")) {

                    }
    }

    private synchronized void initS3() {
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
            if (credentials.getId() == PlatformClientId.S3Bridge) {
                this.accessKey = credentials.getAccessKey();
                this.secretKey = credentials.getSecretKey();
                this.INIT_DONE.set(true);
                return;
            }
        }
        throw new IllegalStateException("No service '" + PlatformClientId.S3Bridge + "' configured into file '" + this.awsCredentialsConfigurationFile.getPath() + "'");
    }
*/
}
