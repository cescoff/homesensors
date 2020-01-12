package com.desi.data.utils;

import com.desi.data.bean.AnnotatedImage;
import com.desi.data.bean.OdometerRecord;
import com.desi.data.config.CarConfiguration;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CarConfigurationHelper {

    private static final String PEUGEOT_305_UUID = "c1e22264-a62e-4662-909f-7c31798d231e";
    private static final String PEUGEOT_305_DISTANCE_UUID = "6f415b87-d7e3-47b3-b744-f4ddab016e6d";
    private static final String PEUGEOT_305_ODOMETER_UUID = "9f62acab-88d8-45fe-96a5-e0c1e62fc27c";
    private static final String PEUGEOT_305_GASOLINE_METER_UUID = "8076a054-82f8-4613-921e-dfa2dc8f2335";
    private static final String PEUGEOT_305_GASOLINE_AVG_METER_UUID = "2402a246-9890-4f85-aef0-9b8a94c55c8d";
    private static final String PEUGEOT_305_GPS_UUID = "bb0d4657-4923-483c-b510-b3dc7cd80cbd";

    private static final String VW_LT25_01_UUID = "94fd7578-cd51-4906-9c03-f456d09d64e8";
    private static final String VW_LT25_01_DISTANCE_UUID = "4beca0b4-41be-4b88-81c1-770656e9c680";
    private static final String VW_LT25_01_ODOMETER_UUID = "6eb81a36-8ee0-4ab9-b816-9824a15812a8";
    private static final String VW_LT25_01_GASOLINE_METER_UUID = "e50b9e89-a537-4634-94e6-e99bb6b9a595";
    private static final String VW_LT25_01_GASOLINE_AVG_METER_UUID = "d4cc3001-9601-41fd-ba7e-23e3646f4142";
    private static final String VW_LT25_01_GPS_UUID = "7450f24e-305d-4a37-afed-021ab0f4954d";

    private static final String[] PEUGEOT_305_ODOMETER_MARKERS = {"PJ74", "JAEGER"};

    private static final Pattern CAR_ODOMETER_PARSER = Pattern.compile("([0-9]{6})");

    public static Iterable<CarConfiguration> all() {
        return ImmutableList.of(getPeugeot305(), getVWLT25());
    }

    public static CarConfiguration resolveUUID(final String uuid) {
// PEUGEOT 305
        if (getPeugeot305().getUUID().equals(uuid)) {
            return getPeugeot305();
        }
        if (getPeugeot305().getDistanceUUID().equals(uuid)) {
            return getPeugeot305();
        }
        if (getPeugeot305().getOdometerUUID().equals(uuid)) {
            return getPeugeot305();
        }
        if (getPeugeot305().getGasolineMeterUUID().equals(uuid)) {
            return getPeugeot305();
        }
        if (getPeugeot305().getGPSUUID().equals(uuid)) {
            return getPeugeot305();
        }
// VW LT25
        if (getVWLT25().getUUID().equals(uuid)) {
            return getPeugeot305();
        }
        if (getVWLT25().getDistanceUUID().equals(uuid)) {
            return getPeugeot305();
        }
        if (getVWLT25().getOdometerUUID().equals(uuid)) {
            return getPeugeot305();
        }
        if (getVWLT25().getGasolineMeterUUID().equals(uuid)) {
            return getPeugeot305();
        }
        if (getVWLT25().getGPSUUID().equals(uuid)) {
            return getPeugeot305();
        }
        throw new IllegalArgumentException("Unknown UUID value '" + uuid + "'");
    }

    public static CarConfiguration getPeugeot305() {
        return new CarConfiguration() {

            @Override
            public String getUUID() {
                return PEUGEOT_305_UUID;
            }

            @Override
            public String getDistanceUUID() {
                return PEUGEOT_305_DISTANCE_UUID;
            }

            @Override
            public String getOdometerUUID() {
                return PEUGEOT_305_ODOMETER_UUID;
            }

            @Override
            public String getGasolineMeterUUID() {
                return PEUGEOT_305_GASOLINE_METER_UUID;
            }

            @Override
            public String getGPSUUID() {
                return PEUGEOT_305_GPS_UUID;
            }

            @Override
            public String getGasolineAvgConsumptionUUID() {
                return PEUGEOT_305_GASOLINE_AVG_METER_UUID;
            }

            @Override
            public boolean isValidGasolineVolume(float value) {
                return value > 5 && value < 50;
            }

            @Override
            public boolean isValidGasolineConsumption(float value) {
                return value >= 6 && value <= 13;
            }

            @Override
            public boolean isValidDistanceBetween2ReFuel(float value) {
                return value > 0 && value < 500;
            }

            @Override
            public boolean isValidGasolinePricePerLitre(float price) {
                return price > 1.4 && price <= 1.9;
            }

            @Override
            public boolean isValidReFuelFullPrice(float value) {
                return value < (1.8 * 45);
            }

            @Override
            public Optional<Float> getOdometerValue(AnnotatedImage image) {
                boolean isOdometerImage = false;
                for (final String text : image.getTextElements()) {
                    for (final String odometerMarker : PEUGEOT_305_ODOMETER_MARKERS) {
                        if (StringUtils.containsIgnoreCase(text, odometerMarker)) {
                            isOdometerImage = true;
                        }
                    }
                }
                if (!isOdometerImage) {
                    return Optional.absent();
                }
                float odometerValue = 0;
                for (final String text : image.getTextElements()) {
                    final Matcher matcher = CAR_ODOMETER_PARSER.matcher(text);
                    if (matcher.find()) {
                        float value = new Float(Integer.parseInt(StringUtils.trim(matcher.group(1))));
                        return Optional.of(value);
                    }
                }

                return Optional.absent();
            }
        };
    }

    public static CarConfiguration getVWLT25() {
        return new CarConfiguration() {

            @Override
            public String getUUID() {
                return VW_LT25_01_UUID;
            }

            @Override
            public String getDistanceUUID() {
                return VW_LT25_01_DISTANCE_UUID;
            }

            @Override
            public String getOdometerUUID() {
                return VW_LT25_01_ODOMETER_UUID;
            }

            @Override
            public String getGasolineMeterUUID() {
                return VW_LT25_01_GASOLINE_METER_UUID;
            }

            @Override
            public String getGPSUUID() {
                return VW_LT25_01_GPS_UUID;
            }

            @Override
            public String getGasolineAvgConsumptionUUID() {
                return VW_LT25_01_GASOLINE_AVG_METER_UUID;
            }

            @Override
            public boolean isValidGasolineVolume(float value) {
                return value > 5 && value < 50;
            }

            @Override
            public boolean isValidGasolineConsumption(float value) {
                return value >= 8 && value <= 18;
            }

            @Override
            public boolean isValidDistanceBetween2ReFuel(float value) {
                return value > 0 && value < 450;
            }

            @Override
            public boolean isValidGasolinePricePerLitre(float price) {
                return price > 1.3 && price < 1.9;
            }

            @Override
            public boolean isValidReFuelFullPrice(float value) {
                return value < (1.8 * 42);
            }

            @Override
            public Optional<Float> getOdometerValue(AnnotatedImage image) {
                return Optional.absent();
            }
        };
    }



}
