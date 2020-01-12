package com.desi.data.config;

import com.desi.data.ImageAnnotator;
import com.desi.data.bean.AnnotatedImage;
import com.desi.data.bean.GPSLatitudeSensorRecord;
import com.desi.data.bean.GPSLongitudeSensorRecord;
import com.desi.data.bean.VehicleImageData;
import com.desi.data.utils.CarConfigurationHelper;
import com.desi.data.utils.JAXBUtils;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import org.apache.commons.lang.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.LocalDateTime;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FuelOCRParser {

    private static final Map<String, String> REPLACEMENETS = ImmutableMap.<String, String>builder().
            put("{", "1").
            put("Ч", "4").
            put("니", "4").
            put("O", "0").
            put("Б", "6").
            put("b", "6").build();

    private static final Pattern PRICE_PATTERN_DEGRAGED = Pattern.compile("([0-9]*[\\s.,]+[0-9]+)\\s*[e€]+.*");
    private static final Pattern PRICE_PATTERN = Pattern.compile("([0-9]+\\.*,*[0-9]*)\\s*[e€]+");
    private static final Pattern DISPLAY_FLOAT_PATTERN = Pattern.compile("([0-9]+\\s*\\.*,*\\s*[0-9]+\\s*)");

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
            return Optional.of(new BasicVehicleImageData(image, odometerValue.get()));
        }
        if (!isFuelImage(image)) {
            return Optional.absent();
        }
        Price pricePerLitre = null;
        Price fullPrice = null;
        float volume = 0;
        List<Float> otherValues = Lists.newArrayList();

        for (final String text : cleanupTexts(image.getTextElements())) {
            final Optional<Price> price = getPrice(text);
            if (price.isPresent()) {
                if (configuration.isValidGasolinePricePerLitre(price.get().value) && price.get().isPricePerLitre) {
                    if (pricePerLitre == null || !pricePerLitre.trusted) {
                        pricePerLitre = price.get();
                    }
                } else if ((fullPrice == null || !fullPrice.trusted) && !price.get().isPricePerLitre) {
                    fullPrice = price.get();
                }
            } else if (getFloatValue(text).isPresent()) {
                otherValues.add(getFloatValue(text).get());
            }
        }

        if (pricePerLitre == null && fullPrice == null && volume == 0 && !hasValidFuelPricePerLitre(otherValues)) {
            otherValues = Ordering.natural().reverse().sortedCopy(Sets.newHashSet(otherValues));
            if (otherValues.size() == 2) {
                fullPrice = new Price(false, otherValues.get(0), false);
                volume = otherValues.get(1);
            } else {
                for (int index = 0; index < otherValues.size(); index++) {
                    for (int test = index; test < otherValues.size(); test++) {
                        if (fullPrice == null) {
                            float ratio = otherValues.get(index) / otherValues.get(test);
                            if (1.4 <= ratio && ratio <= 1.75) {
                                fullPrice = new Price(false, otherValues.get(index), false);
                                volume = otherValues.get(test);
                            }
                        }
                    }
                }
            }
        } else {
            if ((pricePerLitre == null && fullPrice == null) || (pricePerLitre == null && volume == 0) || (fullPrice == null && volume == 0)) {
                otherValues = Ordering.natural().reverse().sortedCopy(Sets.newHashSet(otherValues));
                for (float value : otherValues) {
                    if (configuration.isValidReFuelFullPrice(value) && fullPrice == null) {
                        fullPrice = new Price(false, value, false);
                    } else if (configuration.isValidGasolineVolume(value) && volume == 0) {
                        volume = value;
                    } else if (configuration.isValidGasolinePricePerLitre(value)) {
                        pricePerLitre = new Price(true, value, false);
                    }

                }
                if (fullPrice != null && pricePerLitre != null && volume > 0) {
                    if (pricePerLitre.value * volume != fullPrice.value) {
                        float test1 = pricePerLitre.value * fullPrice.value;
                        if ((volume *10 * 0.95) <=  test1 && test1 <= (volume * 10 * 1.05)) {
                            float tempVolume = fullPrice.value;
                            fullPrice.value = volume * 10;
                            volume = tempVolume;
                        }
                    }
                }
            }
        }

        float volumeEstimate = 0;
        if (fullPrice != null && pricePerLitre != null) {
            volumeEstimate = fullPrice.value / pricePerLitre.value;
        }

        for (int index = 0; index < 3; index++) {
            for (final float value : otherValues) {
                if (configuration.isValidGasolineVolume(value)) {
                    if (volumeEstimate == 0 || ((volumeEstimate * 0.9) < value && value < (volumeEstimate * 1.1))) {
                        if (volume == 0) volume = value;
                    }
                }
                if (configuration.isValidGasolinePricePerLitre(value) && pricePerLitre == null) {
                    pricePerLitre = new Price(true, value, false);
                    if (fullPrice != null) {
                        volumeEstimate = fullPrice.value / pricePerLitre.value;
                    }
                }
                if (configuration.isValidReFuelFullPrice(value) && fullPrice == null) {
                    fullPrice = new Price(false, value, false);
                }
            }
        }

        if (pricePerLitre != null) {
            if (volumeEstimate == 0) {
                volumeEstimate = fullPrice.value / pricePerLitre.value;
            }
            if (volume == 0 || ((volumeEstimate * 0.9) > volume || volume > (volumeEstimate * 1.1))) {
                volume = volumeEstimate;
            }
        }

        final float resultVolume = volume;
        final float resultPrice;
        if (fullPrice != null) {
            resultPrice = fullPrice.value;
        } else {
            resultPrice = 0;
        }
        final float resultPricePerLitre;
        if (pricePerLitre != null) {
            resultPricePerLitre = pricePerLitre.value;
        } else {
            if (resultPrice > 0 && resultVolume > 0 && configuration.isValidGasolinePricePerLitre(resultPrice / resultVolume)) {
                resultPricePerLitre = resultPrice / resultVolume;
            } else {
                resultPricePerLitre = 0;
            }
        }

        return Optional.of(new BasicVehicleImageData(image, resultVolume, resultVolume > 0, resultPrice, resultPrice > 0, resultPricePerLitre, resultPricePerLitre > 0));
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
        }

        return false;
    }

    private Optional<Float> getFloatValue(final String test) {
        final Matcher matcher = DISPLAY_FLOAT_PATTERN.matcher(test);
        if (matcher.find() && matcher.matches()) {
            final String numberValue = StringUtils.replace(StringUtils.trim(matcher.group(1)), " ", "");
            if (StringUtils.containsIgnoreCase(numberValue, ".")) {
                final Float result = Float.parseFloat(StringUtils.trim(numberValue));
                return Optional.of(result);
            } else if (StringUtils.containsIgnoreCase(numberValue, ",")) {
                final Float result = Float.parseFloat(StringUtils.trim(StringUtils.replace(numberValue, ",", ".")));
                return Optional.of(result);
            }
            if (StringUtils.contains(numberValue, " ")) {
                return Optional.of(Float.parseFloat(StringUtils.split(numberValue, " ")[0] + "." + StringUtils.split(numberValue, " ")[1]));
            }
            if (numberValue.length() == 4) {
                if (StringUtils.startsWith(numberValue, "1")) {
                    final Float result = Float.parseFloat(StringUtils.substring(numberValue, 0, 1) + "." + StringUtils.substring(numberValue, 1));
                    if (configuration.isValidGasolinePricePerLitre(result)) {
                        return Optional.of(result);
                    }
                }
                final Float result = Float.parseFloat(StringUtils.substring(numberValue, 0, 2) + "." + StringUtils.substring(numberValue, 2));
                return Optional.of(result);
            }
            if (numberValue.length() == 3) {
                final Float result = Float.parseFloat(StringUtils.substring(numberValue, 0, 1) + "." + StringUtils.substring(numberValue, 1));
                return Optional.of(result);
            }
        }
        return Optional.absent();
    }

    private static class Price {
        private boolean isPricePerLitre = false;
        private float value = 0f;
        private boolean trusted = false;

        public Price(boolean isPricePerLitre, float value, boolean trusted) {
            this.isPricePerLitre = isPricePerLitre;
            this.value = value;
            this.trusted = trusted;
        }

        @Override
        public String toString() {
            return "Price{" +
                    "isPricePerLitre=" + isPricePerLitre +
                    ", value=" + value +
                    ", trusted=" + trusted +
                    '}';
        }
    }

    private Optional<Price> getPrice(final String test) {
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
            parsedValue =
            parsedValue = StringUtils.remove(parsedValue, " ");
            if (StringUtils.startsWith(parsedValue, "1")) {
                return Optional.of(new Price(true, Float.parseFloat(parsedValue), true));
            } else if (StringUtils.startsWith(parsedValue, ".")) {
                return Optional.of(new Price(true, Float.parseFloat("1" + parsedValue), true));
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
                    return Optional.of(new Price(true, value, true));
                } else {
                    if (value > 1 && value < 2) {
                        return Optional.of(new Price(true, value, true));
                    }
                    return Optional.of(new Price(false, value, true));
                }
            }
            if (StringUtils.startsWith(numberValue, "1")) {
                value = Float.parseFloat(StringUtils.substring(numberValue, 0, 1) + "." + StringUtils.substring(numberValue, 1));
                return Optional.of(new Price(true, value, true));
            } else {
                if (numberValue.length() == 4) {
                    value = Float.parseFloat(StringUtils.substring(numberValue, 0, 2) + "." + StringUtils.substring(numberValue, 2));
                    return Optional.of(new Price(false, value, true));
                }
                if (numberValue.length() == 3) {
                    value = Float.parseFloat(StringUtils.substring(numberValue, 0, 1) + "." + StringUtils.substring(numberValue, 1));
                    return Optional.of(new Price(false, value, true));
                }
            }
        }
        return Optional.absent();
    }

    private static class BasicVehicleImageData implements VehicleImageData {

        private final AnnotatedImage source;

        private final float odometerValue;
        private final float volume;
        private final float price;
        private final float pricePerLitre;

        private final boolean hasOdometerValue;
        private final boolean hasVolume;
        private final boolean hasPrice;
        private final boolean hasPricePerLitre;

        private BasicVehicleImageData(AnnotatedImage source, float odometerValue) {
            this.source = source;
            this.odometerValue = odometerValue;
            this.hasOdometerValue = true;

            this.volume = 0;
            this.price = 0;
            this.pricePerLitre = 0;
            this.hasVolume = false;
            this.hasPrice = false;
            this.hasPricePerLitre = false;
        }

        private BasicVehicleImageData(AnnotatedImage source, float volume, boolean hasVolume, float price, boolean hasPrice, float pricePerLitre, boolean hasPricePerLitre) {
            this.source = source;
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
            return new GPSLatitudeSensorRecord("UUID", source.getDateTaken(), source.getLatitudeRef(), source.getLatitude()).getValue();
        }

        @Override
        public float getLongitude() {
            return new GPSLongitudeSensorRecord("UUID", source.getDateTaken(), source.getLongitudeRef(), source.getLongitude()).getValue();
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
        final ImageAnnotator.AnnotatedImageBatch batch = JAXBUtils.unmarshal(ImageAnnotator.AnnotatedImageBatch.class, new File("/Users/corentin/Documents/Developpement/image-annotations.xml"));

        System.out.println("\"FileName\",\"DateTaken\",\"Volume\",\"PricePerLitre\",\"Price\"");
       for (final VehicleImageData vehicleImageData : new FuelOCRParser(CarConfigurationHelper.getPeugeot305()).analyzeImages(batch.getAnnotatedImages())) {
/*            System.out.println("FileName:" + fuelStatistics.getFileName());
            System.out.println("DateTaken:" + fuelStatistics.getDateTaken());
            System.out.println("Price per litre:" + fuelStatistics.getPricePerLitre() + "€/L");
            System.out.println("Price:" + fuelStatistics.getPrice() + "€");
            System.out.println("Volume:" + fuelStatistics.getVolume() + "L");
            System.out.println("_______________________________________________________________");*/
            System.out.println("\"" + vehicleImageData.getFileName() + "\",\"" + vehicleImageData.getDateTaken() + "\"," + vehicleImageData.getVolume() + "," + vehicleImageData.getPricePerLitre() + "," + vehicleImageData.getPrice());
        }

        for (final AnnotatedImage image : batch.getAnnotatedImages()) {
            if ("DSC_0625.JPG".equals(image.getFileName())) {
                new FuelOCRParser(CarConfigurationHelper.getPeugeot305()).analyzeImage(image);
            }
        }

        //
        //
    }

}
