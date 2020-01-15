package com.desi.data.utils;

import com.desi.data.CarSensorRecord;
import com.desi.data.SensorNameProvider;
import com.desi.data.bean.AnnotatedImage;
import com.desi.data.bean.OdometerRecord;
import com.desi.data.bean.VehicleImageData;
import com.desi.data.config.CarConfiguration;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.javatuples.Triplet;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CarConfigurationHelper implements SensorNameProvider {

    private static final String GASOLINE_SP95_98_PRICE_UUID = "defa6525-51f4-4c43-b67d-1caf27f96b4f";
    private static final String GASOLINE_SP95_98_TRIP_PRICE_UUID = GASOLINE_SP95_98_PRICE_UUID;

    private static final String PEUGEOT_305_NAME = "Peugeot 305";
    private static final String PEUGEOT_305_UUID = "c1e22264-a62e-4662-909f-7c31798d231e";
    private static final String PEUGEOT_305_DISTANCE_UUID = "6f415b87-d7e3-47b3-b744-f4ddab016e6d";
    private static final String PEUGEOT_305_TRIP_DISTANCE_UUID = "63d1c24e-893c-46e3-ac9c-c504df31c3e4";
    private static final String PEUGEOT_305_ODOMETER_UUID = "9f62acab-88d8-45fe-96a5-e0c1e62fc27c";
    private static final String PEUGEOT_305_TRIP_GASOLINE_METER_UUID = "598ead65-9250-4dea-aa5c-84bcb960acfb";
    private static final String PEUGEOT_305_GASOLINE_METER_UUID = "8076a054-82f8-4613-921e-dfa2dc8f2335";
    private static final String PEUGEOT_305_GASOLINE_AVG_METER_UUID = "2402a246-9890-4f85-aef0-9b8a94c55c8d";
    private static final String PEUGEOT_305_TRIP_GASOLINE_AVG_METER_UUID = "bca4db3c-45de-49d6-b264-3ffd8eeab56a";
    private static final String PEUGEOT_305_GPS_UUID = "bb0d4657-4923-483c-b510-b3dc7cd80cbd";

    private static final String VW_LT25_01_NAME = "Volkswagen T3";
    private static final String VW_LT25_01_UUID = "94fd7578-cd51-4906-9c03-f456d09d64e8";
    private static final String VW_LT25_01_DISTANCE_UUID = "4beca0b4-41be-4b88-81c1-770656e9c680";
    private static final String VW_LT25_01_TRIP_DISTANCE_UUID = "d34bf7a2-f411-4c6b-8f6d-31aa9ae99881";
    private static final String VW_LT25_01_ODOMETER_UUID = "6eb81a36-8ee0-4ab9-b816-9824a15812a8";
    private static final String VW_LT25_01_GASOLINE_METER_UUID = "e50b9e89-a537-4634-94e6-e99bb6b9a595";
    private static final String VW_LT25_01_TRIP_GASOLINE_METER_UUID = "23c5fda6-44ac-403d-809c-6c218295ff4d";
    private static final String VW_LT25_01_GASOLINE_AVG_METER_UUID = "d4cc3001-9601-41fd-ba7e-23e3646f4142";
    private static final String VW_LT25_01_TRIP_GASOLINE_AVG_METER_UUID = "e702d116-4b6a-4c28-861f-da44fc2c30e5";
    private static final String VW_LT25_01_GPS_UUID = "7450f24e-305d-4a37-afed-021ab0f4954d";

    private static final String[] PEUGEOT_305_ODOMETER_MARKERS = {"PJ74", "PJ 74", "JAEGER"};

    private static final String[] PEUGEOT_305_CAR_TAGS = {"PEUGEOT_305_BREAK", "EB-124-ZT"};
    private static final String[] VW_LT25_01_CAR_TAGS = {"VOLKSWAGEN_LT25", "DQ-190-ZD"};

    private static final Pattern PEUGEOT_305_ODOMETER_PARSER = Pattern.compile("([0-9]{6})");
    private static final Pattern PEUGEOT_305ODOMETER_PATTERN_DEGRAGED = Pattern.compile("[0-9]+[\\s.,]+[0-9]+");

    private static final CarConfigurationHelper INSTANCE = new CarConfigurationHelper();

    private static final Triplet<Float, Float, Float> HOME_POSITION = Triplet.with(48.813521f, 2.383957f, 80.0f);

    private static final float TRIP_DISTANCE_THRESHOLD = 100000;

    private CarConfigurationHelper() {}

    public static CarConfigurationHelper getInstance() {
        return INSTANCE;
    }

    public static Iterable<CarConfiguration> all() {
        return ImmutableList.of(getPeugeot305(), getVWLT25());
    }

    public static CarConfiguration resolveUUID(final String uuid) {
// PEUGEOT 305
        if (getPeugeot305().getUUID().equals(uuid)) {
            return getPeugeot305();
        }
        if (PEUGEOT_305_DISTANCE_UUID.equals(uuid)) {
            return getPeugeot305();
        }
        if (PEUGEOT_305_TRIP_DISTANCE_UUID.equals(uuid)) {
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
        if (VW_LT25_01_DISTANCE_UUID.equals(uuid)) {
            return getPeugeot305();
        }
        if (VW_LT25_01_TRIP_DISTANCE_UUID.equals(uuid)) {
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
        if (GASOLINE_SP95_98_PRICE_UUID.equals(uuid)) {
            return getPeugeot305();
        }
        throw new IllegalArgumentException("Unknown UUID value '" + uuid + "'");
    }

    public static CarConfiguration getPeugeot305() {
        return new CarConfiguration() {

            @Override
            public String getName() {
                return PEUGEOT_305_NAME;
            }

            @Override
            public String getUUID() {
                return PEUGEOT_305_UUID;
            }

            @Override
            public String getDistanceUUID(final CarSensorRecord startPosition, final CarSensorRecord endPosition) {
                if (isTrip(startPosition) || isTrip(endPosition)) return PEUGEOT_305_TRIP_DISTANCE_UUID;
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
            public String getGasolineAvgConsumptionUUID(final CarSensorRecord startPosition, final CarSensorRecord endPosition) {
                if (isTrip(startPosition) || isTrip(endPosition)) return PEUGEOT_305_TRIP_GASOLINE_AVG_METER_UUID;
                return PEUGEOT_305_GASOLINE_AVG_METER_UUID;
            }

            @Override
            public String getGasolineUUID(final CarSensorRecord startPosition, final CarSensorRecord endPosition) {
                if (isTrip(startPosition) || isTrip(endPosition)) return GASOLINE_SP95_98_TRIP_PRICE_UUID;
                return GASOLINE_SP95_98_PRICE_UUID;
            }

            @Override
            public boolean isVehicleInATrip(CarSensorRecord carSensorRecord) {
                return isTrip(carSensorRecord);
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
                return 10 < value && value < (1.8 * 45);
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
                    final Matcher matcher = PEUGEOT_305_ODOMETER_PARSER.matcher(text);
                    if (matcher.find()) {
                        float value = new Float(Integer.parseInt(StringUtils.trim(matcher.group(1))));
                        return Optional.of(value);
                    }
                    final Matcher degradatedParser = PEUGEOT_305ODOMETER_PATTERN_DEGRAGED.matcher(text);
                    if (degradatedParser.matches()) {
                        float value = new Float(
                                Integer.parseInt(
                                        StringUtils.replaceEach(
                                                StringUtils.trim(text),
                                                new String[] {".", ",", " "},
                                                new String[] {"", "", ""})));
                        return Optional.of(value);
                    }
                }

                return Optional.absent();
            }

            @Override
            public boolean isVehicleInImage(AnnotatedImage image) {
                for (final String text : image.getTextElements()) {
                    for (final String tag : PEUGEOT_305_CAR_TAGS) {
                        if (StringUtils.containsIgnoreCase(text, tag)) {
                            return true;
                        }
                    }
                }
                return false;
            }

        };
    }

    public static CarConfiguration getVWLT25() {
        return new CarConfiguration() {

            @Override
            public String getName() {
                return VW_LT25_01_NAME;
            }

            @Override
            public String getUUID() {
                return VW_LT25_01_UUID;
            }

            @Override
            public String getDistanceUUID(final CarSensorRecord startPosition, final CarSensorRecord endPosition) {
                if (isTrip(startPosition) || isTrip(endPosition)) return VW_LT25_01_TRIP_DISTANCE_UUID;
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
            public String getGasolineAvgConsumptionUUID(final CarSensorRecord startPosition, final CarSensorRecord endPosition) {
                if (isTrip(startPosition) || isTrip(endPosition)) return VW_LT25_01_TRIP_GASOLINE_AVG_METER_UUID;
                return VW_LT25_01_GASOLINE_AVG_METER_UUID;
            }

            @Override
            public String getGasolineUUID(final CarSensorRecord startPosition, final CarSensorRecord endPosition) {
                if (isTrip(startPosition) || isTrip(endPosition)) return GASOLINE_SP95_98_TRIP_PRICE_UUID;
                return GASOLINE_SP95_98_PRICE_UUID;
            }

            @Override
            public boolean isVehicleInATrip(CarSensorRecord carSensorRecord) {
                return isTrip(carSensorRecord);
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
                return 10 < value && value < (1.8 * 42);
            }

            @Override
            public Optional<Float> getOdometerValue(AnnotatedImage image) {
                return Optional.absent();
            }



            @Override
            public boolean isVehicleInImage(AnnotatedImage image) {
                for (final String text : image.getTextElements()) {
                    for (final String tag : VW_LT25_01_CAR_TAGS) {
                        if (StringUtils.containsIgnoreCase(text, tag)) {
                            return true;
                        }
                    }
                }
                return false;
            }

        };
    }

    public static Float calculateDistanceFromHome(float latitude, float longitude, float altitude) {
        if (latitude == 0 && longitude == 0) return 0f;
        return DistanceUtils.getDistance(Triplet.with(latitude, longitude, altitude), HOME_POSITION);
    }

    private static Float getDistanceFromHome(final CarSensorRecord carSensorRecord) {
        return calculateDistanceFromHome(carSensorRecord.getLatitude(), carSensorRecord.getLongitude(), carSensorRecord.getAltitude());
    }

    private static boolean isTrip(final CarSensorRecord carSensorRecord) {
        return getDistanceFromHome(carSensorRecord) > TRIP_DISTANCE_THRESHOLD;
    }

    @Override
    public String getDisplayName(String uuid) {
        final CarConfiguration carConfiguration = resolveUUID(uuid);
        if (carConfiguration == null) {
            return uuid;
        }
        return carConfiguration.getName();
    }

    @Override
    public Map<String, String> getDisplayNames(Iterable<String> uuids) {
        final ImmutableMap.Builder<String, String> result = ImmutableMap.builder();
        for (final String uuid : uuids) result.put(uuid, getDisplayName(uuid));
        return result.build();
    }
}
