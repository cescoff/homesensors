package com.desi.data.binding;

import com.desi.data.bean.IGasStation;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.joda.time.LocalDateTime;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.*;
import java.util.List;

@XmlRootElement(name = "pdv")
@XmlAccessorType(XmlAccessType.FIELD)
public class GasStation implements IGasStation {

    @XmlAttribute
    private long id;

    @XmlAttribute(name = "latitude")
    private String latitudeStr;

    @XmlAttribute(name = "longitude")
    private String longitudeStr;

    @XmlAttribute(name = "cp")
    private long codePostal;

    @XmlAttribute(name = "pop")
    private String pop;

    @XmlElement
    private String adresse;

    @XmlElement
    private String ville;

    @XmlElementWrapper(name = "service") @XmlElement(name = "services")
    private List<String> services = Lists.newArrayList();

    @XmlElement(name = "prix")
    private List<Prix> prix = Lists.newArrayList();

    @Override
    public String getAddress() {
        StringBuilder result = new StringBuilder();
        if (StringUtils.isNotEmpty(adresse)) {
            result.append(adresse);
            if (StringUtils.isNotEmpty(ville)) result.append(" ");
        }
        if (StringUtils.isNotEmpty(ville)) {
            result.append(ville);
        }
        return ville;
    }

    public float getLatitude() {
        if (StringUtils.isEmpty(latitudeStr)) {
//            LoggerFactory.getLogger(getClass()).error("Empty latitude for station '" + getAddress() + "'");
            return 0;
        }
        try {
            return Float.parseFloat(StringUtils.substring(latitudeStr, 0, 2) + "." + StringUtils.substring(StringUtils.remove(latitudeStr, "."), 2));
        } catch (Throwable t) {
//            LoggerFactory.getLogger(getClass()).error("Cannot parse latitude value '" + latitudeStr + "'", t);
        }
        return 0f;
    }

    public float getLongitude() {
        if (StringUtils.isEmpty(longitudeStr)) {
//            LoggerFactory.getLogger(getClass()).error("Empty longitude for station '" + getAddress() + "'");
            return 0;
        }
        if (StringUtils.startsWith(longitudeStr, "-")) {
            return Float.parseFloat(StringUtils.substring(longitudeStr, 0, 2) + "." + StringUtils.substring(StringUtils.remove(longitudeStr, "."), 2));
        }
        return Float.parseFloat(StringUtils.substring(longitudeStr, 0, 1) + "." + StringUtils.substring(StringUtils.remove(longitudeStr, "."), 1));
    }

    public Optional<Float> getFuelPrice(final FuelType fuelType, final LocalDateTime dateTaken) {
        if (Iterables.isEmpty(prix)) {
            return Optional.absent();
        }
        Prix firstOfYear = null;
        Prix previous = null;
        for (final Prix price : this.prix) {
            if (price.getNom() == fuelType) {
                if (previous != null) {
                    if (StringUtils.isNotEmpty(previous.getMaj()) && StringUtils.isNotEmpty(price.getMaj())) {
                        if ((previous.getDateTime().equals(dateTaken) || previous.getDateTime().isBefore(dateTaken)) && dateTaken.isBefore(price.getDateTime())) {
                            return Optional.of(previous.getPrice());
                        }
                    }
                } else {
                    firstOfYear = price;
                }
                previous = price;
            }
        }
        if (previous != null
                && previous.getDateTime().isBefore(dateTaken)
                && dateTaken.isBefore(
                        LocalDateTime.now()
                                .withYear(previous.getDateTime().getYear()).
                                withMonthOfYear(12).
                                withDayOfMonth(31).
                                plusDays(1))) {
            return Optional.of(previous.getPrice());
        }

        if (firstOfYear != null && dateTaken.isBefore(firstOfYear.getDateTime()) && dateTaken.isAfter(LocalDateTime.now().withYear(firstOfYear.getDateTime().getYear()).withMonthOfYear(1).withDayOfMonth(1).minusDays(1))) {
            return Optional.of(firstOfYear.getPrice());
        }

        return Optional.absent();
    }

    @XmlAccessorType(XmlAccessType.FIELD)
    public static class Prix {

        @XmlAttribute
        private FuelType nom;

        @XmlAttribute
        private int id;

        @XmlAttribute
        private String maj;

        @XmlAttribute
        private String valeur;

        public FuelType getNom() {
            return nom;
        }

        public int getId() {
            return id;
        }

        public String getMaj() {
            return maj;
        }

        public LocalDateTime getDateTime() {
            if (StringUtils.isEmpty(maj)) {
                return null;
            }
            return new org.joda.time.LocalDateTime(maj);
        }

        public String getValeur() {
            return valeur;
        }

        public float getPrice() {
            return Float.parseFloat(StringUtils.substring(valeur, 0, 1) + "." + StringUtils.substring(valeur, 1));
        }

    }

}
