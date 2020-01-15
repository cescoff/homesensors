package com.desi.data.vision;

import com.desi.data.config.PlatformCredentialsConfig;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.automl.v1beta1.*;
import com.google.cloud.vision.v1.*;
import com.google.cloud.vision.v1.Image;
import com.google.protobuf.ByteString;import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class VisionConnector {

    private static Logger logger = LoggerFactory.getLogger(VisionConnector.class);

    private final String autoMLProjectId;
    private final String autoMLModelId;

    private ImageAnnotatorClient vision = null;
    private PredictionServiceClient autoML = null;

    public VisionConnector(String autoMLProjectId, String autoMLModelId) {
        this.autoMLProjectId = autoMLProjectId;
        this.autoMLModelId = autoMLModelId;
    }

    public boolean begin(final PlatformCredentialsConfig.Credentials credentials, final File configDir) throws IOException {
        if (this.vision != null) {
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
        this.vision =
                ImageAnnotatorClient.create(ImageAnnotatorSettings.newBuilder().setCredentialsProvider(FixedCredentialsProvider.create(googleCredentials)).build());
        this.autoML =
                PredictionServiceClient.create(PredictionServiceSettings.newBuilder().setCredentialsProvider(FixedCredentialsProvider.create(googleCredentials)).build());

        return true;

    }

    public Iterable<String> getRawAnnotations(final byte[] imgBytes) {
        // Builds the image annotation request
        List<AnnotateImageRequest> requests = new ArrayList<>();
        Image img = Image.newBuilder().setContent(ByteString.copyFrom(imgBytes)).build();
        Feature feat = Feature.newBuilder().setType(Feature.Type.TEXT_DETECTION).build();
        AnnotateImageRequest request = AnnotateImageRequest.newBuilder()
                .addFeatures(feat)
                .setImage(img)
                .build();
        requests.add(request);

        // Performs label detection on the image file
        BatchAnnotateImagesResponse response = vision.batchAnnotateImages(requests);
        List<AnnotateImageResponse> responses = response.getResponsesList();

        final List<String> result = Lists.newArrayList();

        for (AnnotateImageResponse res : responses) {
            if (res.hasError()) {
                logger.error("Cannot get vision annotation : " + res.getError().getMessage());

                return Collections.emptyList();
            }

            for (EntityAnnotation annotation : res.getTextAnnotationsList()) {
                annotation.getAllFields().forEach((k, v) ->
                        append(result, k, v.toString()));
            }
        }

        if (StringUtils.isNotEmpty(autoMLProjectId) && StringUtils.isNotEmpty(autoMLModelId)) {
            ModelName name = ModelName.of(autoMLProjectId, "us-central1", autoMLModelId);

            com.google.cloud.automl.v1beta1.Image image = com.google.cloud.automl.v1beta1.Image.newBuilder().setImageBytes(ByteString.copyFrom(imgBytes)).build();

            ExamplePayload payload = ExamplePayload.newBuilder().setImage(image).build();

            if (payload != null) {
                PredictRequest predictRequest =
                        PredictRequest.newBuilder()
                                .setName(name.toString())
                                .setPayload(payload)
                                .putParams(
                                        "score_threshold", "0.8") // [0.0-1.0] Only produce results higher than this value
                                .build();

                PredictResponse autoMLResponse = autoML.predict(predictRequest);

                for (AnnotationPayload annotationPayload : autoMLResponse.getPayloadList()) {
                    result.add(annotationPayload.getDisplayName());
                }
            }
        }

        return ImmutableList.copyOf(result);
    }

    private void append(final List<String> values, final Descriptors.FieldDescriptor descriptor, final String value) {
        if ("STRING".equals(descriptor.getType().name())) {
            values.add(value);
        }
    }

}
