package com.desi.data.utils;

import com.desi.data.SensorRecord;
import com.desi.data.bean.GasolineVolumeRecord;
import com.desi.data.bean.OdometerRecord;
import com.desi.data.bean.UnreadableCarOdometerRecord;
import com.desi.data.bean.UnreadableGasolineVolumeRecord;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import org.apache.commons.lang.StringUtils;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class ImageAnalyzer {

    private static final String UNKNOWN_ODOMETER_UUID = "52f88258-fbd6-419f-9fc6-286724d9fbc6";
    private static final String UNKNOWN_GASOLINE_METER_UUID = "54dd4a04-26d8-447b-a595-6665b014d929";

    private static final String PEUGEOT_305_ODOMETER_UUID = "9f62acab-88d8-45fe-96a5-e0c1e62fc27c";
    private static final String VW_LT25_01_ODOMETER_UUID = "6eb81a36-8ee0-4ab9-b816-9824a15812a8";

    private static final String PEUGEOT_305_GASOLINE_METER_UUID = "8076a054-82f8-4613-921e-dfa2dc8f2335";
    private static final String VW_LT25_01_GASOLINE_METER_UUID = "e50b9e89-a537-4634-94e6-e99bb6b9a595";

    private static final String GASOLINE_PRICE_UUID = "9762b419-c5bd-4285-bce9-496485e3b710";

    private static final Pattern CAR_ODOMETER_PARSER = Pattern.compile("([0-9]{6})");
    private static final Pattern GASOLINE_VOLUME_PARSER = Pattern.compile("([0-9]{2}[,\\.]+[0-9]{0,2})");

    private static Logger logger = LoggerFactory.getLogger(ImageAnalyzer.class);

    private ImageAnalyzer() {}

    public static Optional<SensorRecord> analyze(final LocalDateTime imageDateTime, final Iterable<String> texts) {
        boolean carOdometerImage = false;
        boolean gasolineVolumeImage = false;
        for (final String text : texts) {
            if (StringUtils.containsIgnoreCase(text, "PJ74") || StringUtils.containsIgnoreCase(text, "JAEGER")) {
                carOdometerImage = true;
            }
            if (StringUtils.containsIgnoreCase(text, "LITRE") || StringUtils.containsIgnoreCase(text, "VOLUME")) {
                gasolineVolumeImage = true;
            }
        }
        if (carOdometerImage) {
            logger.info("Car odometer found");
            for (final String text : texts) {
                final Matcher matcher = CAR_ODOMETER_PARSER.matcher(text);
                if (matcher.find()) {
                    logger.info("Found car odometer value : " + matcher.group(1));
                    return Optional.of(new OdometerRecord(new Float(Integer.parseInt(StringUtils.trim(matcher.group(1)))), PEUGEOT_305_ODOMETER_UUID, imageDateTime));
                }
            }
            return Optional.of(new UnreadableCarOdometerRecord(imageDateTime, PEUGEOT_305_ODOMETER_UUID));
        }
        if (gasolineVolumeImage) {
            logger.info("Gasoline volume found");
            for (int position = 0; position < Iterables.size(texts); position++) {
                if (StringUtils.containsIgnoreCase(Iterables.get(texts, position), "Litre") ||
                        ((position + 1) < Iterables.size(texts) && StringUtils.containsIgnoreCase(Iterables.get(texts, position+1), "Litre"))) {
                    final Matcher matcher = GASOLINE_VOLUME_PARSER.matcher(Iterables.get(texts, position));

                    if (matcher.find()) {
                        final String gasolineVolumeValue = StringUtils.replace(matcher.group(1), ",", ".");
                        logger.info("Found gasoline volume value : '" + gasolineVolumeValue);
                        return Optional.of(new GasolineVolumeRecord(imageDateTime, Float.parseFloat(gasolineVolumeValue), UNKNOWN_GASOLINE_METER_UUID));
                    } else {
                        final Matcher fakeIntMatcher = Pattern.compile("([0-9]{4})").matcher(Iterables.get(texts, position));
                        if (fakeIntMatcher.find()) {
                            final String gasolineVolumeValue = StringUtils.substring(fakeIntMatcher.group(1), 0, 2) + "." + StringUtils.substring(fakeIntMatcher.group(1), 2);
                            logger.info("Found gasoline volume value : " + gasolineVolumeValue);
                            return Optional.of(new GasolineVolumeRecord(imageDateTime, Float.parseFloat(gasolineVolumeValue), UNKNOWN_GASOLINE_METER_UUID));
                        }
                    }
                }
            }
            logger.info("Unreadable gasoline volume value : '" + Joiner.on("', '").join(texts) + "'");
            return Optional.of(new UnreadableGasolineVolumeRecord(imageDateTime, UNKNOWN_GASOLINE_METER_UUID, "'" + Joiner.on("', '").join(texts) + "'"));
        }
        return Optional.absent();
    }


}
