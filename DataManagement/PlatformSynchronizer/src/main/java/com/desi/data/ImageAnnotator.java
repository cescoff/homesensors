package com.desi.data;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.desi.data.bean.*;
import com.desi.data.config.PlatformCredentialsConfig;
import com.desi.data.utils.JAXBUtils;
import com.desi.data.vision.VisionConnector;
import com.drew.imaging.ImageMetadataReader;
import com.drew.imaging.ImageProcessingException;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.Tag;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.log4j.PropertyConfigurator;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.*;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ImageAnnotator {

    private static final String EXIF_GPS_LATITUDE_REF = "[GPS] GPS Latitude Ref - ";
    private static final String EXIF_GPS_LATITUDE = "[GPS] GPS Latitude - ";
    private static final String EXIF_GPS_LONGITUDE_REF = "[GPS] GPS Longitude Ref - ";
    private static final String EXIF_GPS_LONGITUDE = "[GPS] GPS Longitude - ";
    private static final String EXIF_GPS_ALTITUDE = "[GPS] GPS Altitude - ";

    private static final DateTimeFormatter EXIF_DATE_TIME_FORMAT = DateTimeFormat.forPattern("yyyy:MM:dd HH:mm:ss");
    private static final DateTimeFormatter OUTPUT_DATE_TIME_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

    private static final Pattern DATETIME_FILE_NAME_PATTERN = Pattern.compile("IMG_([0-9]{8})_([0-9]{6}).jpg");

    private static Logger logger = LoggerFactory.getLogger(ImageAnnotator.class);

    private static final String REGION = "us-east-1";

    private final List<String> s3BucketNames;

    private final File credentialsFile;

    private final File storageFile;

    private final PlatformClientId clientId;

    private String accessKey;

    private String secretKey;

    private PlatformCredentialsConfig credentialsConfig;

    private AtomicBoolean INIT_DONE = new AtomicBoolean(false);

    private VisionConnector visionConnector = null;

    private Set<String> knownFileNames = Sets.newHashSet();

    private AnnotatedImageBatch annotatedImageBatch = new AnnotatedImageBatch();

    public ImageAnnotator(List<String> s3BucketNames, File credentialsFile, File storageFile, PlatformClientId clientId) {
        this.s3BucketNames = s3BucketNames;
        this.credentialsFile = credentialsFile;
        this.clientId = clientId;
        this.storageFile = storageFile;
    }

    public Iterable<AnnotatedImage> annotate(final LocalDateTime checkPoint) {
        init();

        final List<AnnotatedImage> annotatedImages = Lists.newArrayList();

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().
                withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey))).
                withRegion(REGION).
                build();


        for (final String s3BucketName : s3BucketNames) {
            final ListObjectsV2Result result = s3.listObjectsV2(s3BucketName);

            final List<S3ObjectSummary> objects = result.getObjectSummaries();

            for (S3ObjectSummary os : objects) {
                try {
                    final Optional<LocalDateTime> guessedFileDateTime = guessDateTimeFromKey(os.getKey());

                    if (!knownFileNames.contains(FilenameUtils.getName(os.getKey())) && acceptFileName(os.getKey()) && (!guessedFileDateTime.isPresent() || guessedFileDateTime.get().isAfter(checkPoint))) {
                        S3Object fullObject = s3.getObject(new GetObjectRequest(os.getBucketName(), os.getKey()));

                        if (new LocalDateTime(fullObject.getObjectMetadata().getLastModified()).isAfter(checkPoint)) {
                            logger.info("Parsing content for object s3://" + os.getBucketName() + "/" + os.getKey());
                            final Optional<AnnotatedImage> annotation = annotateImage(fullObject.getObjectContent(), os.getKey(), checkPoint);
                            if (annotation.isPresent()) {
                                annotatedImages.add(annotation.get());
                                this.annotatedImageBatch.getAnnotatedImages().add(annotation.get());
                            }
                            //records.addAll(parseContent(fullObject.getObjectContent()));
                        } else {
                            logger.info("File '" + os.getKey() + "' ignored because it is too old");
                        }
                    } else {
                        if (knownFileNames.contains(FilenameUtils.getName(os.getKey()))) {
                            logger.info("File '" + os.getKey() + "' ignored because it has already been annotated");
                        } else if (acceptFileName(os.getKey())) {
                            logger.info("File '" + os.getKey() + "' ignored because it is too old");
                        }
                    }
                } catch (Exception e) {
                    logger.error("Failed to parse file s3://" + os.getBucketName() + "/" + os.getKey() + ": " + e.getMessage(), e);
                }
                if (annotatedImages.size() > 0 && (annotatedImages.size() % 2) == 0) {
                    store();
                }
            }
        }

        s3.shutdown();

        store();

        return ImmutableList.copyOf(annotatedImages);
    }

    private void store() {
        logger.info("Storing images into file '" + this.storageFile.getAbsolutePath() + "'");
        try {
            JAXBUtils.marshal(this.annotatedImageBatch, this.storageFile, true);
        } catch (JAXBException e) {
            throw new IllegalStateException("Cannot marshall storage file '" + this.storageFile.getAbsolutePath() + "'", e);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot marshall storage file '" + this.storageFile.getAbsolutePath() + "'", e);
        }
    }

    private boolean acceptFileName(final String key) {
        return StringUtils.containsIgnoreCase(key, ".jpg")
                || StringUtils.containsIgnoreCase(key, ".jpeg");
    }

    private Optional<LocalDateTime> guessDateTimeFromKey(final String key) {
        final Matcher dateTimeFileNameMatcher = DATETIME_FILE_NAME_PATTERN.matcher(key);
        if (dateTimeFileNameMatcher.find()) {
            return Optional.of(LocalDateTime.parse(dateTimeFileNameMatcher.group(1).substring(0, 4) + "-" + dateTimeFileNameMatcher.group(1).substring(4, 6) + "-" + dateTimeFileNameMatcher.group(1).substring(6) + "T" + dateTimeFileNameMatcher.group(2).substring(0, 1) + ":" + dateTimeFileNameMatcher.group(2).substring(2, 3) + ":" + dateTimeFileNameMatcher.group(2).substring(4)));
        }
        return Optional.absent();
    }

    private Optional<AnnotatedImage> annotateImage(final InputStream inputStream, final String fileName, final LocalDateTime checkPoint) throws IOException, ImageProcessingException {
        ByteArrayOutputStream imageInMemory = new ByteArrayOutputStream();
        IOUtils.copy(inputStream, imageInMemory);
        LocalDateTime imageDateTime = null;
        String latitude = null;
        String latitudeRef = null;
        String longitude = null;
        String longitudeRef = null;
        String altitude = null;
        Metadata metadata = ImageMetadataReader.readMetadata(new ByteArrayInputStream(imageInMemory.toByteArray()));
        for (Directory directory : metadata.getDirectories()) {
            for (Tag tag : directory.getTags()) {
                if (tag.getTagType() == 306) {
                    imageDateTime = EXIF_DATE_TIME_FORMAT.parseLocalDateTime(tag.getDescription());
                }
                final String tagString = tag.toString();
                if (StringUtils.contains(tagString, EXIF_GPS_LATITUDE_REF)) {
                    latitudeRef = StringUtils.remove(tagString, EXIF_GPS_LATITUDE_REF);
                } else if (StringUtils.contains(tagString, EXIF_GPS_LONGITUDE_REF)) {
                    longitudeRef = StringUtils.remove(tagString, EXIF_GPS_LONGITUDE_REF);
                } else if (StringUtils.contains(tagString, EXIF_GPS_LATITUDE)) {
                    latitude = StringUtils.remove(tagString, EXIF_GPS_LATITUDE);
                } else if (StringUtils.contains(tagString, EXIF_GPS_LONGITUDE)) {
                    longitude = StringUtils.remove(tagString, EXIF_GPS_LONGITUDE);
                } else if (StringUtils.contains(tagString, EXIF_GPS_ALTITUDE)) {
                    altitude = StringUtils.remove(tagString, EXIF_GPS_ALTITUDE);
                }
            }
        }
        if (imageDateTime != null) {
            if (imageDateTime.isBefore(checkPoint)) {
                return Optional.absent();
            }
            logger.info("Image date time : " + OUTPUT_DATE_TIME_FORMAT.print(imageDateTime));
            BufferedImage inputImage = ImageIO.read(new ByteArrayInputStream(imageInMemory.toByteArray()));
            int newWidth = 2048;
            int newHeight = new Double((newWidth * inputImage.getHeight()) / inputImage.getWidth()).intValue();
            if (inputImage.getWidth() < inputImage.getHeight()) {
                newHeight = 2048;
                newWidth = new Double((newHeight * inputImage.getWidth()) / inputImage.getHeight()).intValue();
            }
            BufferedImage outputImage = new BufferedImage(newWidth,
                    newHeight, inputImage.getType());
            Graphics2D g2d = outputImage.createGraphics();
            g2d.drawImage(inputImage, 0, 0, newWidth, newHeight, null);
            g2d.dispose();

            final ByteArrayOutputStream imageForGoogleCloudVisionInMemory = new ByteArrayOutputStream();
            // writes to output file
            ImageIO.write(outputImage, "jpg", imageForGoogleCloudVisionInMemory);

            if (visionConnector != null) {
                final Iterable<String> texts = visionConnector.getRawAnnotations(imageForGoogleCloudVisionInMemory.toByteArray());

                final AnnotatedImage result = new AnnotatedImage();
                result.setLatitudeRef(latitudeRef);
                result.setLatitude(latitude);
                result.setLongitudeRef(longitudeRef);
                result.setLongitude(longitude);
                result.setFileName(fileName);
                result.setDateTaken(imageDateTime);
                result.setAltitude(altitude);
                Iterables.addAll(result.getTextElements(), texts);

                return Optional.of(result);
            } else {
                logger.error("Vision connector is not available");
            }
        } else {
            logger.info("Image date time : null");
        }
        return Optional.absent();
    }

    private synchronized void init() {
        if (INIT_DONE.get()) {
            return;
        }
        if (!this.credentialsFile.exists()) {
            throw new IllegalStateException("Credentials file '" + this.credentialsFile.getPath() + "' does not exist");
        }

        logger.info("Searching for already annotated images into storage file '" + this.storageFile + "'");
        if (this.storageFile.exists()) {
            AnnotatedImageBatch batch = null;
            try {
                batch = JAXBUtils.unmarshal(AnnotatedImageBatch.class, this.storageFile);
            } catch (JAXBException e) {
                logger.error("Cannot unmarshall storage file '" + this.storageFile.getAbsolutePath() + "'", e);
            } catch (IOException e) {
                logger.error("Cannot unmarshall storage file '" + this.storageFile.getAbsolutePath() + "'", e);
            }
            if (batch != null) {
                Iterables.addAll(this.knownFileNames, Iterables.transform(batch.getAnnotatedImages(), new Function<AnnotatedImage, String>() {
                    @Nullable
                    @Override
                    public String apply(@Nullable AnnotatedImage annotatedImage) {
                        return annotatedImage.getFileName();
                    }
                }));
                this.annotatedImageBatch.getAnnotatedImages().addAll(batch.getAnnotatedImages());
            }
        }

        try {
            credentialsConfig = JAXBUtils.unmarshal(PlatformCredentialsConfig.class, this.credentialsFile);
        } catch (Throwable t) {
            throw new IllegalStateException("Malformed file '" + this.credentialsFile.getPath() + "'", t);
        }

        visionConnector = new VisionConnector();
        PlatformCredentialsConfig.Credentials candidate = null;
        for (final PlatformCredentialsConfig.Credentials configuredCredentials : this.credentialsConfig.getCredentials()) {
            if (configuredCredentials.getId() == PlatformClientId.BigQuery) {
                candidate = configuredCredentials;
            }
        }
        if (candidate != null) {
            try {
                visionConnector.begin(candidate, this.credentialsFile.getParentFile());
                logger.info("Google Cloud Vision connector instanciated successfully");
            } catch (IOException e) {
                logger.error("Cannot initiate vision connector", e);
            }
        } else {
            logger.error("No credentials found for vision service");
        }


        for (final PlatformCredentialsConfig.Credentials credentials : credentialsConfig.getCredentials()) {
            if (credentials.getId() == this.clientId) {
                this.accessKey = credentials.getAccessKey();
                this.secretKey = credentials.getSecretKey();
                this.INIT_DONE.set(true);
                return;
            }
        }

        throw new IllegalStateException("No service '" + this.clientId + "' configured into file '" + this.credentialsFile.getPath() + "'");
    }


    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage " + S3Bridge.class.getSimpleName() + " <CREDENTIALS_FILE_PATH>");
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

            final File storageDir = new File(SystemUtils.getUserDir(), "carsensors");
            if (!storageDir.exists()) storageDir.mkdir();

            final Iterable<AnnotatedImage> annotatedImages = new ImageAnnotator(Lists.newArrayList("desi-legacy-counters-images" /*, */ /*"desi-counters-images"*/), new File(args[0]), new File(storageDir, "image-annotations.xml"), PlatformClientId.S3Bridge).annotate(LocalDateTime.parse("2010-1-1T00:00:00"));
            final AnnotatedImageBatch batch = new AnnotatedImageBatch();
            Iterables.addAll(batch.getAnnotatedImages(), annotatedImages);

            final File outputFile = new File("temp-image-annotations.xml");
            try {
                JAXBUtils.marshal(batch, outputFile, true);
                logger.info("Annotations saved into file '" + outputFile.getAbsolutePath() + "'");
            } catch (Throwable t) {
                logger.error("Cannot marshall image annotations", t);
            }

        } catch (Throwable t) {
            logger.error("An error has occured", t);
            System.exit(4);
        }
        System.exit(1);
    }

    @XmlRootElement(name = "annotated-images")
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class AnnotatedImageBatch {

        @XmlElement(name = "image")
        private List<AnnotatedImage> annotatedImages = Lists.newArrayList();

        public List<AnnotatedImage> getAnnotatedImages() {
            return annotatedImages;
        }
    }


}
