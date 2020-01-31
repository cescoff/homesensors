package com.desi.data.config;

import com.desi.data.ImageAnnotator;
import com.desi.data.SensorType;
import com.desi.data.SensorUnit;
import com.desi.data.bean.*;
import com.desi.data.binding.FuelType;
import com.desi.data.utils.CarConfigurationHelper;
import com.desi.data.utils.JAXBUtils;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FuelOCRParser {

    private static final Map<String, String> REPLACEMENETS = ImmutableMap.<String, String>builder().
            put("{", "1").
            put("Ч", "4").
            put("니", "4").
            put("O", "0").
            put("Б", "6").
            put("b", "6").
            put("|", "")
        .build();

    private static final Pattern PRICE_PATTERN_DEGRAGED = Pattern.compile("([0-9]*[\\s.,]+[0-9]+)\\s*[e€]+.*");
    private static final Pattern PRICE_PATTERN = Pattern.compile("([0-9]+\\.*,*[0-9]*)\\s*[e€]+");
    private static final Pattern DISPLAY_FLOAT_PATTERN = Pattern.compile("([0-9]+\\s*\\.*,*\\s*[0-9]+\\s*[0-9]*)\\s*[a-zA-Z]*");

    private static final Logger logger = LoggerFactory.getLogger(FuelOCRParser.class);

    private final CarConfiguration configuration;

    public FuelOCRParser(CarConfiguration configuration) {
        this.configuration = configuration;
    }

    public Iterable<VehicleImageData> analyzeImages(final Iterable<AnnotatedImage> images) {
        return Iterables.transform(Iterables.filter(Iterables.transform(images, new Function<AnnotatedImage, Optional<VehicleImageData>>() {
            @Nullable
            @Override
            public Optional<VehicleImageData> apply(@Nullable AnnotatedImage annotatedImage) {
                return analyzeImage(annotatedImage);
            }
        }), new Predicate<Optional<VehicleImageData>>() {
            @Override
            public boolean apply(@Nullable Optional<VehicleImageData> fuelStatisticsOptional) {
                return fuelStatisticsOptional.isPresent();
            }
        }), new Function<Optional<VehicleImageData>, VehicleImageData>() {
            @Nullable
            @Override
            public VehicleImageData apply(@Nullable Optional<VehicleImageData> fuelStatisticsOptional) {
                return fuelStatisticsOptional.get();
            }
        });
    }

    public Optional<VehicleImageData> analyzeImage(final AnnotatedImage image) {
        final Optional<Float> odometerValue = configuration.getOdometerValue(image);
        if (odometerValue.isPresent()) {
            return Optional.of(new BasicVehicleImageData(image, configuration.getUUID(), odometerValue.get()));
        }
        if (!isFuelImage(image)) {
            if (configuration.isVehicleInImage(image)) {
                return Optional.of(new BasicVehicleImageData(image, configuration.getUUID()));
            } else {
                return Optional.absent();
            }
        }
        Value pricePerLitre = null;
        Value fullPrice = null;
        Value volume = null;
        List<Value> otherValues = Lists.newArrayList();

        for (final String text : cleanupTexts(image.getTextElements())) {
            final List<Value> prices = getPrice(text);
            if (prices.size() > 0) {
                for (final Value price : prices) {
                    if (price.type == ValueType.PRICE_PER_LITRE) {
                        pricePerLitre = price;
                    } else if (price.type == ValueType.FULL_PRICE) {
                        fullPrice = price;
                    } else {
                        if (otherValues.size() == 0 && configuration.isValidReFuelFullPrice(price.value)) {
                            fullPrice = new Value(ValueType.FULL_PRICE, price.value, false);
                        }
                        otherValues.add(price);
                    }
                }
            } else if (getFloatValue(text).size() > 0) {
                if (otherValues.size() == 0) {
                    float price = Ordering.natural().reverse().onResultOf(new Function<Value, Float>() {
                        @Nullable
                        @Override
                        public Float apply(@Nullable Value value) {
                            return value.value;
                        }
                    }).sortedCopy(getFloatValue(text)).get(0).value;
                    if (configuration.isValidReFuelFullPrice(price)) {
                        fullPrice = new Value(ValueType.FULL_PRICE, price, false);
                    }
                }
                otherValues.addAll(getFloatValue(text));
            }
        }

        final Optional<IGasStation> gasStation = configuration.findGasStation(image);
        if (gasStation.isPresent()) {
            logger.debug("Found gas station '" + gasStation.get().getAddress() + "' for fuel event made on '" + image.getDateTaken() + "'");
            final Optional<Float> price = gasStation.get().getFuelPrice(configuration.getFuelType(), image.getDateTaken());
            if (price.isPresent()) {
                logger.debug("Gas station '" + gasStation.get().getAddress() + "' has value '" + price.get() + "' on type '" + configuration.getFuelType() + "' for fuel event made on '" + image.getDateTaken() + "'");
                if (pricePerLitre == null || !pricePerLitre.trusted) {
                    pricePerLitre = new Value(ValueType.PRICE_PER_LITRE, price.get(), true);
                } else {
                    if (!fuzzyEquals(pricePerLitre.value, price.get())) {
                        logger.warn("Not equals values '" + pricePerLitre.value + "!=" + price.get());
                    }
                    pricePerLitre = new Value(ValueType.PRICE_PER_LITRE, price.get(), true);
                }
            }
        }

        otherValues = cleanupOtherValues(otherValues, pricePerLitre, fullPrice, volume);

        float resultfullPrice = 0;
        float resultVolume = 0;
        float resultPricePerLitre = 0;

        for (final Value otherValue : otherValues) {
            if (otherValue.type == ValueType.FULL_PRICE) {
                resultfullPrice = otherValue.value;
            } else if (otherValue.type == ValueType.VOLUME) {
                resultVolume = otherValue.value;
            } else if (otherValue.type == ValueType.PRICE_PER_LITRE) {
                resultPricePerLitre = otherValue.value;
            }
        }

        if (resultVolume == 0 && resultPricePerLitre == 0) {
            for (final Value otherValue : otherValues) {
                if (resultfullPrice == 0 && otherValue.type == ValueType.PRICE && configuration.isValidGasolinePricePerLitre(otherValue.value)) {
                    resultPricePerLitre = otherValue.value;
                }
                if (resultVolume == 0 && otherValue.type == ValueType.UNKNOWN && configuration.isValidGasolineVolume(otherValue.value)) {
                    resultVolume = otherValue.value;
                }
            }
            resultfullPrice = resultVolume * resultPricePerLitre;
        }

        return Optional.of(new BasicVehicleImageData(image, configuration.getUUID(), resultVolume, resultVolume > 0, resultfullPrice, resultfullPrice > 0, resultPricePerLitre, resultPricePerLitre > 0));
    }

    private List<Value> cleanupOtherValues(final List<Value> values, final Value pricePerLitre, final Value fullPrice, final Value volume) {
        final List<Value> unique = Lists.newArrayList(Iterables.filter(Sets.newHashSet(values), new Predicate<Value>() {
            @Override
            public boolean apply(@Nullable Value aFloat) {
                if (pricePerLitre != null && pricePerLitre.trusted && pricePerLitre.value == aFloat.value) {
                    return false;
                }
                if (fullPrice != null && fullPrice.trusted && fullPrice.value == aFloat.value) {
                    return false;
                }
                if (volume != null && volume.value == aFloat.value) {
                    return false;
                }
                return true;
            }
        }));
        if (pricePerLitre != null) {
            if (fullPrice != null) {
                final float volumeValue = fullPrice.value / pricePerLitre.value;
                for (final Value otherValue : unique) {
                    if (fuzzyEquals(volumeValue, otherValue.value)) {
                        return ImmutableList.of(new Value(ValueType.FULL_PRICE, fullPrice.value,true),
                                new Value(ValueType.VOLUME, otherValue.value, true),
                                new Value(ValueType.PRICE_PER_LITRE, pricePerLitre.value, true));
                    }
                }
                if (configuration.isValidGasolineVolume(volumeValue)) {
                    return ImmutableList.of(new Value(ValueType.FULL_PRICE, fullPrice.value,false),
                            new Value(ValueType.VOLUME, volumeValue, false),
                            new Value(ValueType.PRICE_PER_LITRE, pricePerLitre.value, false));
                }
            } else if (volume != null) {
                final float fullPriceValue = pricePerLitre.value * volume.value;
                for (final Value otherValue : unique) {
                    if (fuzzyEquals(fullPriceValue, otherValue.value)) {
                        return ImmutableList.of(new Value(ValueType.FULL_PRICE, otherValue.value,true),
                                new Value(ValueType.VOLUME, volume.value, true),
                                new Value(ValueType.PRICE_PER_LITRE, pricePerLitre.value, true));
                    }
                }
                if (configuration.isValidReFuelFullPrice(fullPriceValue)) {
                    return ImmutableList.of(new Value(ValueType.FULL_PRICE, fullPriceValue,false),
                            new Value(ValueType.VOLUME, volume.value, false),
                            new Value(ValueType.PRICE_PER_LITRE, pricePerLitre.value, false));
                }
            }
            final Iterable<Value> multiplied = Iterables.transform(values, Multiply(pricePerLitre.value));
            final Iterable<Value> divided = Iterables.transform(values, Divide(pricePerLitre.value));

            Value fullPriceResult = null;
            Value volumeResult = null;
            for (final Value otherValue : values) {
                Iterable<Value> test = Iterables.filter(multiplied, FuzzyEqualsFilter(otherValue.value));
                if (!Iterables.isEmpty(test)) {
                    final Iterable<Value> fullPriceCandidate = Iterables.filter(values, FuzzyEqualsFilter(Iterables.getFirst(test, null).value));
                    fullPriceResult = new Value(ValueType.FULL_PRICE, Iterables.getFirst(fullPriceCandidate, null).value, true);
                    final Iterable<Value> volumeCandidate = Iterables.filter(values, FuzzyEqualsFilter(fullPriceResult.value / pricePerLitre.value));
                    volumeResult = new Value(ValueType.VOLUME, Iterables.getFirst(Iterables.filter(values, FuzzyEqualsFilter(Iterables.getFirst(volumeCandidate, null).value)), null).value, true);
                    return ImmutableList.of(fullPriceResult, volumeResult, pricePerLitre);
                } else {
                    test = Iterables.filter(divided, FuzzyEqualsFilter(otherValue.value));
                    if (!Iterables.isEmpty(test)) {
                        final Iterable<Value> volumeCandidate = Iterables.filter(values, FuzzyEqualsFilter(Iterables.getFirst(test, null).value));
                        volumeResult = new Value(ValueType.VOLUME, Iterables.getFirst(Iterables.filter(values, FuzzyEqualsFilter(Iterables.getFirst(volumeCandidate, null).value)), null).value, true);
                        final Iterable<Value> fullPriceCandidate = Iterables.filter(values, FuzzyEqualsFilter(volumeResult.value * pricePerLitre.value));
                        fullPriceResult = new Value(ValueType.FULL_PRICE, Iterables.getFirst(fullPriceCandidate, null).value, true);
                        return ImmutableList.of(fullPriceResult, volumeResult, pricePerLitre);
                    }
                }
            }

        } else if (fullPrice != null) {
            if (volume != null) {
                final float pricePerLitreValue = fullPrice.value / volume.value;
                for (final Value otherValue : unique) {
                    if (fuzzyEquals(pricePerLitreValue, otherValue.value)) {
                        return ImmutableList.of(new Value(ValueType.FULL_PRICE, fullPrice.value,true),
                                new Value(ValueType.VOLUME, volume.value, true),
                                new Value(ValueType.PRICE_PER_LITRE, otherValue.value, true));
                    }
                }
            }

            Value pricePerLitreResult = null;
            Value volumeResult = null;
            for (final Value otherValue : values) {
                Iterable<Value> volumeCandidates = Iterables.filter(values, FuzzyEqualsFilter(fullPrice.value / otherValue.value));
                if (!Iterables.isEmpty(volumeCandidates)) {
                    if (!configuration.isValidGasolinePricePerLitre(Iterables.getFirst(Iterables.filter(values, FuzzyEqualsFilter(Iterables.getFirst(volumeCandidates, null).value)), null).value)
                            && configuration.isValidGasolinePricePerLitre(otherValue.value)) {
                        volumeResult = new Value(ValueType.VOLUME, Iterables.getFirst(Iterables.filter(values, FuzzyEqualsFilter(Iterables.getFirst(volumeCandidates, null).value)), null).value, true);
                        pricePerLitreResult = new Value(ValueType.PRICE_PER_LITRE, otherValue.value, true);
                    } else if (configuration.isValidGasolinePricePerLitre(Iterables.getFirst(Iterables.filter(values, FuzzyEqualsFilter(Iterables.getFirst(volumeCandidates, null).value)), null).value)) {
                        pricePerLitreResult = new Value(ValueType.PRICE_PER_LITRE, Iterables.getFirst(Iterables.filter(values, FuzzyEqualsFilter(Iterables.getFirst(volumeCandidates, null).value)), null).value, true);
                        volumeResult = new Value(ValueType.VOLUME, otherValue.value, true);
                    }
                    if (volumeResult != null && pricePerLitreResult != null) {
                        return ImmutableList.of(fullPrice,
                                volumeResult,
                                pricePerLitreResult);
                    }
                }
            }
            for (final Value otherValue : values) {
                if (otherValue.type == ValueType.PRICE && configuration.isValidGasolinePricePerLitre(otherValue.value)) {
                    pricePerLitreResult = new Value(ValueType.PRICE_PER_LITRE, otherValue.value, false);
                }
            }
            if (pricePerLitreResult != null) {
                return ImmutableList.of(fullPrice, new Value(ValueType.VOLUME, fullPrice.value / pricePerLitreResult.value, false), pricePerLitreResult);
            }
        } else if (volume != null) {
            Value pricePerLitreResult = null;
            Value fullPriceResult = null;
            for (final Value otherValue : values) {
                Iterable<Value> fullPriceCandidates = Iterables.filter(values, FuzzyEqualsFilter(volume.value * otherValue.value));
                if (!Iterables.isEmpty(fullPriceCandidates)) {
                    fullPriceResult = new Value(ValueType.VOLUME, Iterables.getFirst(Iterables.filter(values, FuzzyEqualsFilter(Iterables.getFirst(fullPriceCandidates, null).value)), null).value, true);
                    pricePerLitreResult = new Value(ValueType.PRICE_PER_LITRE, otherValue.value, true);
                    return ImmutableList.of(fullPriceResult,
                            volume,
                            pricePerLitreResult);

                }
            }
        }

        if (values.size() >= 3) {
            for (final Value value : Lists.newArrayList(values)) {
                for (final Value otherValue : values) {
                    if (value.value != otherValue.value) {
                        Iterable<Value> multiplied = Iterables.filter(values, FuzzyEqualsFilter(value.value * otherValue.value));
                        if (!Iterables.isEmpty(multiplied)) {
                            Value pricePerLitreResult = null;
                            Value volumeResult = null;
                            Value fullPriceResult = new Value(ValueType.FULL_PRICE, Iterables.getFirst(Iterables.filter(values, FuzzyEqualsFilter(Iterables.getFirst(multiplied, null).value)), null).value, true);
                            if (configuration.isValidGasolinePricePerLitre(value.value)) {
                                pricePerLitreResult = new Value(ValueType.PRICE_PER_LITRE, value.value, true);
                                volumeResult = new Value(ValueType.VOLUME, otherValue.value, true);
                            } else {
                                pricePerLitreResult = new Value(ValueType.PRICE_PER_LITRE, otherValue.value, true);
                                volumeResult = new Value(ValueType.VOLUME, value.value, true);
                            }
                            if (configuration.isValidGasolinePricePerLitre(pricePerLitreResult.value)) {
                                return ImmutableList.of(fullPriceResult, volumeResult, pricePerLitreResult);
                            }
                        }
                    }
                }
            }
        }

        // Price per litre is not available in values
        if (fullPrice != null) {
            for (final Value value : values) {
                if (configuration.isValidGasolinePricePerLitre(fullPrice.value / value.value)) {
                    return ImmutableList.of(
                            fullPrice,
                            new Value(ValueType.VOLUME, value.value, false),
                            new Value(ValueType.PRICE_PER_LITRE, fullPrice.value / value.value, false)
                    );
                }
            }
            for (final Value value : values) {
                if (configuration.isValidGasolinePricePerLitre(value.value)) {
                    return ImmutableList.of(
                            fullPrice,
                            new Value(ValueType.VOLUME, fullPrice.value / value.value, false),
                            new Value(ValueType.PRICE_PER_LITRE, value.value, false)
                    );
                }
            }
        }

        return Ordering.natural().reverse().onResultOf(new Function<Value, Float>() {
            @Nullable
            @Override
            public Float apply(@Nullable Value value) {
                return value.value;
            }
        }).sortedCopy(values);
    }

    private Function<Value, Value> Multiply(final float test) {
        return new Function<Value, Value>() {
            @Nullable
            @Override
            public Value apply(@Nullable Value value) {
                return new Value(value.type, value.value * test, false);
            }
        };
    }

    private Function<Value, Value> Divide(final float test) {
        return new Function<Value, Value>() {
            @Nullable
            @Override
            public Value apply(@Nullable Value value) {
                return new Value(value.type, value.value / test, false);
            }
        };
    }

    private Predicate<Value> FuzzyEqualsFilter(final Float test) {
        return new Predicate<Value>() {
            @Override
            public boolean apply(@Nullable Value value) {
                return fuzzyEquals(test, value.value);
            }
        };
    }

    private boolean fuzzyEquals(final Float test, final Float value) {
        if (test == 0 && value == 0) {
            return true;
        }
        return (test * 0.95) < value && value < (test * 1.05);
    }

    private boolean hasValidFuelPricePerLitre(final Iterable<Float> values) {
        for (final Float value : values) {
            if (configuration.isValidGasolinePricePerLitre(value)) {
                return true;
            }
        }
        return false;
    }

    private Iterable<String> cleanupTexts(final Iterable<String> texts) {
        final List<String> result = Lists.newArrayList();
        for (final String text : texts) {
            if (StringUtils.contains(text, "\n")) {
                for (final String split : StringUtils.split(text, "\n")) {
                    result.add(cleanupText(split));
                }
            } else {
                result.add(cleanupText(text));
            }
        }
        return result;
    }

    private String cleanupText(final String text) {
        String result = text;
        for (final String element : REPLACEMENETS.keySet()) {
            result = StringUtils.replace(result, element, REPLACEMENETS.get(element));
        }
        return result;
    }

    private boolean isFuelImage(final AnnotatedImage annotatedImage) {
        for (final String text : annotatedImage.getTextElements()) {
            if (StringUtils.containsIgnoreCase(text, "LITR")) {
                return true;
            }
            if (StringUtils.containsIgnoreCase(text, "SP98")) {
                return true;
            }
            if (StringUtils.containsIgnoreCase(text, "SP9")) {
                return true;
            }
            if (StringUtils.containsIgnoreCase(text, "SP")) {
                return true;
            }
        }

        return false;
    }

    private List<Value> getFloatValue(final String test) {
        final Matcher matcher = DISPLAY_FLOAT_PATTERN.matcher(test);
        if (matcher.find() && matcher.matches()) {
            final String numberValue = StringUtils.replace(StringUtils.trim(matcher.group(1)), " ", "");
            if (StringUtils.containsIgnoreCase(numberValue, ".")) {
                final Float result = Float.parseFloat(StringUtils.trim(numberValue));
                return ImmutableList.of(new Value(guessType(test), result, true));
            } else if (StringUtils.containsIgnoreCase(numberValue, ",")) {
                final Float result = Float.parseFloat(StringUtils.trim(StringUtils.replace(numberValue, ",", ".")));
                return ImmutableList.of(new Value(guessType(test), result, true));
            }
            if (StringUtils.contains(numberValue, " ")) {
                return ImmutableList.of(new Value(guessType(test), Float.parseFloat(StringUtils.split(numberValue, " ")[0] + "." + StringUtils.split(numberValue, " ")[1]), true));
            }
            if (numberValue.length() == 4) {
                if (StringUtils.startsWith(numberValue, "1")) {
                    final Float result = Float.parseFloat(StringUtils.substring(numberValue, 0, 1) + "." + StringUtils.substring(numberValue, 1));
                    if (configuration.isValidGasolinePricePerLitre(result)) {
                        return ImmutableList.of(new Value(guessType(test), result, false), new Value(guessType(test), result * 10, false));
                    }
                }
                final Float result = Float.parseFloat(StringUtils.substring(numberValue, 0, 2) + "." + StringUtils.substring(numberValue, 2));
                return ImmutableList.of(new Value(guessType(test), result, false), new Value(guessType(test), result / 10, false));
            }
            if (numberValue.length() == 3) {
                final Float result = Float.parseFloat(StringUtils.substring(numberValue, 0, 1) + "." + StringUtils.substring(numberValue, 1));
                return ImmutableList.of(new Value(guessType(test), result, false), new Value(guessType(test), result * 10, false));
            }
        }
        return Collections.emptyList();
    }

    private ValueType guessType(final String test) {
        if (Pattern.compile("[0-9]+\\s*[€e]+.*").matcher(test).matches()) {
            return ValueType.PRICE;
        }
        if (StringUtils.containsIgnoreCase(test, "LITR")) {
            return ValueType.VOLUME;
        }
        return ValueType.UNKNOWN;
    }

    private enum ValueType {
        PRICE(true),
        FULL_PRICE(true),
        PRICE_PER_LITRE(true),
        VOLUME(false),
        UNKNOWN(false);

        private final boolean price;

        ValueType(boolean price) {
            this.price = price;
        }

        public boolean isPrice() {
            return this.price;
        }

    }

    private static class Value {
        private ValueType type;
        private float value = 0f;
        private boolean trusted = false;

        public Value(ValueType type, float value, boolean trusted) {
            this.type = type;
            this.value = value;
            if (type == ValueType.VOLUME) {
                this.trusted = true;
            } else {
                this.trusted = false;
            }
        }

        @Override
        public String toString() {
            return "Value{" +
                    "type=" + type +
                    ", value=" + value +
                    ", trusted=" + trusted +
                    '}';
        }
    }

    private List<Value> getPrice(final String test) {
        final Matcher degragedPriceMatcher = PRICE_PATTERN_DEGRAGED.matcher(test);
        if (degragedPriceMatcher.matches()) {
            String parsedValue = StringUtils.replace(test, ",", ".");
            if (StringUtils.indexOf(parsedValue, "e") > 0) {
                parsedValue = StringUtils.substring(parsedValue, 0, StringUtils.indexOf(parsedValue, "e"));
            }
            if (StringUtils.indexOf(parsedValue, "€") > 0) {
                parsedValue = StringUtils.substring(parsedValue, 0, StringUtils.indexOf(parsedValue, "€"));
            }
            if (StringUtils.indexOf(parsedValue, " ") == 1) {
                parsedValue = StringUtils.substring(parsedValue, 0, 1) + "." + StringUtils.substring(parsedValue, 1);
            }
            parsedValue = StringUtils.remove(parsedValue, " ");
            if (StringUtils.startsWith(parsedValue, "1")) {
                return ImmutableList.of(new Value(ValueType.PRICE_PER_LITRE, Float.parseFloat(parsedValue), true));
            } else if (StringUtils.startsWith(parsedValue, ".")) {
                return ImmutableList.of(new Value(ValueType.PRICE_PER_LITRE, Float.parseFloat("1" + parsedValue), true));
            }
        }

        final Matcher matcher = PRICE_PATTERN.matcher(test);
        if (matcher.find()) {
            final String numberValue = StringUtils.trim(matcher.group(1));
            float value = 0f;
            if (StringUtils.containsIgnoreCase(numberValue, ".")) {
                value = Float.parseFloat(StringUtils.trim(numberValue));
            } else if (StringUtils.containsIgnoreCase(numberValue, ",")) {
                value = Float.parseFloat(StringUtils.trim(StringUtils.replace(numberValue, ",", ".")));
            }
            if (value > 0) {
                if (StringUtils.containsIgnoreCase(test, "litr")) {
                    return ImmutableList.of(new Value(ValueType.PRICE_PER_LITRE, value, true));
                } else {
                    return ImmutableList.of(new Value(ValueType.PRICE, value, true));
                }
            }
            if (StringUtils.startsWith(numberValue, "1")) {
                value = Float.parseFloat(StringUtils.substring(numberValue, 0, 1) + "." + StringUtils.substring(numberValue, 1));
                return ImmutableList.of(new Value(ValueType.PRICE, value, false), new Value(ValueType.PRICE, value * 10, false));
            } else {
                if (numberValue.length() == 4) {
                    value = Float.parseFloat(StringUtils.substring(numberValue, 0, 2) + "." + StringUtils.substring(numberValue, 2));
                    return ImmutableList.of(new Value(ValueType.PRICE, value, false));
                }
                if (numberValue.length() == 3) {
                    value = Float.parseFloat(StringUtils.substring(numberValue, 0, 1) + "." + StringUtils.substring(numberValue, 1));
                    return ImmutableList.of(new Value(ValueType.PRICE, value, false), new Value(ValueType.PRICE, value * 10, false));
                }
            }
        }
        return Collections.emptyList();
    }

    private static class BasicVehicleImageData implements VehicleImageData {

        private final AnnotatedImage source;

        private final String uuid;

        private final float odometerValue;
        private final float volume;
        private final float price;
        private final float pricePerLitre;

        private final boolean hasOdometerValue;
        private final boolean hasVolume;
        private final boolean hasPrice;
        private final boolean hasPricePerLitre;

        private BasicVehicleImageData(AnnotatedImage source, String uuid) {
            this.source = source;
            this.uuid = uuid;
            this.odometerValue = 0;
            this.hasOdometerValue = false;

            this.volume = 0;
            this.price = 0;
            this.pricePerLitre = 0;
            this.hasVolume = false;
            this.hasPrice = false;
            this.hasPricePerLitre = false;
        }

        private BasicVehicleImageData(AnnotatedImage source, String uuid, float odometerValue) {
            this.source = source;
            this.uuid = uuid;
            this.odometerValue = odometerValue;
            this.hasOdometerValue = true;

            this.volume = 0;
            this.price = 0;
            this.pricePerLitre = 0;
            this.hasVolume = false;
            this.hasPrice = false;
            this.hasPricePerLitre = false;
        }

        private BasicVehicleImageData(AnnotatedImage source, String uuid, float volume, boolean hasVolume, float price, boolean hasPrice, float pricePerLitre, boolean hasPricePerLitre) {
            this.source = source;
            this.uuid = uuid;
            this.odometerValue = 0;
            this.hasOdometerValue = false;

            this.volume = volume;
            this.price = price;
            this.pricePerLitre = pricePerLitre;
            this.hasVolume = hasVolume;
            this.hasPrice = hasPrice;
            this.hasPricePerLitre = hasPricePerLitre;

        }

        @Override
        public String getUUID() {
            return uuid;
        }

        @Override
        public float getOdometerValue() {
            return odometerValue;
        }

        @Override
        public float getVolume() {
            return volume;
        }

        @Override
        public float getPrice() {
            return price;
        }

        @Override
        public float getPricePerLitre() {
            return pricePerLitre;
        }

        @Override
        public boolean hasVolume() {
            return hasVolume;
        }

        @Override
        public boolean hasPrice() {
            return hasPrice;
        }

        @Override
        public boolean hasPricePerLitre() {
            return hasPricePerLitre;
        }

        @Override
        public boolean hasOdometerValue() {
            return hasOdometerValue;
        }

        @Override
        public String getFileName() {
            return source.getFileName();
        }

        @Override
        public LocalDateTime getDateTaken() {
            return source.getDateTaken();
        }

        @Override
        public float getLatitude() {
            if (StringUtils.isEmpty(source.getLatitude())) {
                return 0;
            }
            return new GPSLatitudeSensorRecord("UUID", source.getDateTaken(), source.getLatitudeRef(), source.getLatitude()).getValue();
        }

        @Override
        public float getLongitude() {
            if (StringUtils.isEmpty(source.getLongitude())) {
                return 0;
            }
            return new GPSLongitudeSensorRecord("UUID", source.getDateTaken(), source.getLongitudeRef(), source.getLongitude()).getValue();
        }

        @Override
        public float getAltitude() {
            if (StringUtils.isNotEmpty(source.getAltitude()) && StringUtils.containsIgnoreCase(source.getAltitude(), " metres")) {
                return Float.parseFloat(StringUtils.remove(source.getAltitude(), " metres"));
            }
            return 0;
        }

        @Override
        public VehicleImageData getImageData() {
            return this;
        }

        @Override
        public float getValue() {
            return 0;
        }

        @Override
        public String getSensorUUID() {
            return "uuid";
        }

        @Override
        public SensorUnit getUnit() {
            return SensorUnit.POSITION;
        }

        @Override
        public SensorType getType() {
            return SensorType.DISTANCE_ODOMETER;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof BasicVehicleImageData)) return false;
            BasicVehicleImageData that = (BasicVehicleImageData) o;
            return Float.compare(that.getOdometerValue(), getOdometerValue()) == 0 &&
                    Float.compare(that.getVolume(), getVolume()) == 0 &&
                    Float.compare(that.getPrice(), getPrice()) == 0 &&
                    Float.compare(that.getPricePerLitre(), getPricePerLitre()) == 0 &&
                    hasOdometerValue == that.hasOdometerValue &&
                    hasVolume == that.hasVolume &&
                    hasPrice == that.hasPrice &&
                    hasPricePerLitre == that.hasPricePerLitre &&
                    Objects.equals(source, that.source);
        }

        @Override
        public int hashCode() {
            return Objects.hash(source, getOdometerValue(), getVolume(), getPrice(), getPricePerLitre(), hasOdometerValue, hasVolume, hasPrice, hasPricePerLitre);
        }

        @Override
        public String toString() {
            return "BasicVehicleImageData{" +
                    "date=" + source.getDateTaken() +
                    ", fileName=" + source.getFileName() +
                    '}';
        }
    }

    public static void main(String[] args) throws JAXBException, IOException {
/*        System.out.println(new FuelOCRParser(CarConfigurationHelper.getPeugeot305()).getPrice("1589 e"));
        System.out.println(new FuelOCRParser(CarConfigurationHelper.getPeugeot305()).getPrice("1478 €"));
        System.out.println(new FuelOCRParser(CarConfigurationHelper.getPeugeot305()).getPrice("1.478 €"));
        System.out.println(new FuelOCRParser(CarConfigurationHelper.getPeugeot305()).getPrice("1478 e"));
        System.out.println(new FuelOCRParser(CarConfigurationHelper.getPeugeot305()).getPrice("1.478 e"));
        System.out.println(new FuelOCRParser(CarConfigurationHelper.getPeugeot305()).getPrice("1657€"));
        System.out.println(new FuelOCRParser(CarConfigurationHelper.getPeugeot305()).getPrice("1.657€"));
        System.out.println(new FuelOCRParser(CarConfigurationHelper.getPeugeot305()).getPrice("1657e"));
        System.out.println(new FuelOCRParser(CarConfigurationHelper.getPeugeot305()).getPrice("1.657e"));
        System.out.println(new FuelOCRParser(CarConfigurationHelper.getPeugeot305()).getPrice("1657l"));
        System.out.println(new FuelOCRParser(CarConfigurationHelper.getPeugeot305()).getPrice("1.657l"));*/
        System.out.println(new FuelOCRParser(CarConfigurationHelper.getPeugeot305()).getPrice(",759 "));
        System.out.println(new FuelOCRParser(CarConfigurationHelper.getPeugeot305()).getPrice(",759 e"));
        System.out.println(new FuelOCRParser(CarConfigurationHelper.getPeugeot305()).getPrice("1 759 e"));
        final ImageAnnotator.AnnotatedImageBatch batch = JAXBUtils.unmarshal(ImageAnnotator.AnnotatedImageBatch.class, ConfigurationUtils.getAnnotationsFile());

        final LineIterator lineIterator = new LineIterator(new InputStreamReader(new FileInputStream(new File(ConfigurationUtils.getStorageDir(), "awaited-results.csv"))));

        final Map<String, VehicleImageData> awaitedResults = Maps.newHashMap();
        final Set<String> validFileNames = Sets.newHashSet();
        if (lineIterator.hasNext()) lineIterator.nextLine();
        while (lineIterator.hasNext()) {

            final String[] splits = StringUtils.split(lineIterator.nextLine(), ",");
            if (splits.length >= 6) {
                final String fileName = splits[0];
                final LocalDateTime dateTime = new LocalDateTime(splits[1]);
                final Float volume = Float.parseFloat(splits[2]);
                final Float pricePerLitre = Float.parseFloat(splits[3]);
                final Float fullPrice = Float.parseFloat(splits[4]);
                if ("OK".equals(splits[5])) {
                    validFileNames.add(fileName);
                }
                awaitedResults.put(fileName, new VehicleImageData() {

                    @Override
                    public String getUUID() {
                        return null;
                    }

                    @Override
                    public float getOdometerValue() {
                        return 0;
                    }

                    @Override
                    public float getVolume() {
                        return volume;
                    }

                    @Override
                    public float getPrice() {
                        return fullPrice;
                    }

                    @Override
                    public float getPricePerLitre() {
                        return pricePerLitre;
                    }

                    @Override
                    public boolean hasVolume() {
                        return true;
                    }

                    @Override
                    public boolean hasPrice() {
                        return true;
                    }

                    @Override
                    public boolean hasPricePerLitre() {
                        return true;
                    }

                    @Override
                    public boolean hasOdometerValue() {
                        return false;
                    }

                    @Override
                    public String getFileName() {
                        return fileName;
                    }

                    @Override
                    public LocalDateTime getDateTaken() {
                        return dateTime;
                    }

                    @Override
                    public float getLatitude() {
                        return 0;
                    }

                    @Override
                    public float getLongitude() {
                        return 0;
                    }

                    @Override
                    public float getAltitude() {
                        return 0;
                    }

                    @Override
                    public VehicleImageData getImageData() {
                        return this;
                    }

                    @Override
                    public float getValue() {
                        return 0;
                    }

                    @Override
                    public String getSensorUUID() {
                        return "uuid";
                    }

                    @Override
                    public SensorUnit getUnit() {
                        return SensorUnit.POSITION;
                    }

                    @Override
                    public SensorType getType() {
                        return SensorType.POSITION;
                    }
                });
            }
        }

       final FileOutputStream fileOutputStream = new FileOutputStream(new File(ConfigurationUtils.getStorageDir(), "test.csv"));
        fileOutputStream.write("\"FileName\",\"DateTaken\",\"OdometerValue\",\"Volume\",\"PricePerLitre\",\"Price\"".getBytes());
        FuelOCRParser parser = new FuelOCRParser(CarConfigurationHelper.getPeugeot305());
       for (final VehicleImageData vehicleImageData : parser.analyzeImages(batch.getAnnotatedImages())) {
            if (vehicleImageData.getOdometerValue() == 0) {
                if (awaitedResults.containsKey(vehicleImageData.getFileName())) {
                    if (parser.fuzzyEquals(awaitedResults.get(vehicleImageData.getFileName()).getVolume(), vehicleImageData.getVolume())
                            && parser.fuzzyEquals(awaitedResults.get(vehicleImageData.getFileName()).getPricePerLitre(), vehicleImageData.getPricePerLitre())
                            && parser.fuzzyEquals(awaitedResults.get(vehicleImageData.getFileName()).getPrice(), vehicleImageData.getPrice())) {
                        //System.out.println("OK[" + vehicleImageData.getFileName() + "]");
                        if (!validFileNames.contains(vehicleImageData.getFileName())) {
                            System.out.println("UPGRADE[" + vehicleImageData.getFileName() + "]" + "\"" + vehicleImageData.getDateTaken() + "\"," + vehicleImageData.getOdometerValue() + "," + vehicleImageData.getVolume() + "," + vehicleImageData.getPricePerLitre() + "," + vehicleImageData.getPrice());
                        }
                    } else if (validFileNames.contains(vehicleImageData.getFileName())) {
                        System.out.println("\"" + vehicleImageData.getFileName() + "\",\"" + vehicleImageData.getDateTaken() + "\"," + vehicleImageData.getOdometerValue() + "," + vehicleImageData.getVolume() + "," + vehicleImageData.getPricePerLitre() + "," + vehicleImageData.getPrice());
                        fileOutputStream.write(new String("\"" + vehicleImageData.getFileName() + "\",\"" + vehicleImageData.getDateTaken() + "\"," + vehicleImageData.getOdometerValue() + "," + vehicleImageData.getVolume() + "," + vehicleImageData.getPricePerLitre() + "," + vehicleImageData.getPrice() + "\n").getBytes());
                    }
                } else {
                    System.out.println("Unknown file date : '" + vehicleImageData.getFileName() + "'");
                }
            }
        }
        fileOutputStream.close();
        for (final AnnotatedImage image : batch.getAnnotatedImages()) {
            if ("DSC_2706.JPG".equals(image.getFileName())) {
                new FuelOCRParser(CarConfigurationHelper.getPeugeot305()).analyzeImage(image);
            }
        }

        //
        //
    }

}
