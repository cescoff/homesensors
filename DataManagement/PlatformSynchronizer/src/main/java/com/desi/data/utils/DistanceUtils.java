package com.desi.data.utils;

import com.desi.data.bean.GPSLatitudeSensorRecord;
import com.desi.data.bean.GPSLongitudeSensorRecord;
import com.google.common.base.Optional;
import org.apache.commons.lang.StringUtils;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.joda.time.LocalDateTime;


public class DistanceUtils {

    private static int earth_radius_in_meter = 6371 * 1000;

    public static float getDistance(Triplet<Float, Float, Float> firstPointLatLngAlt, Triplet<Float, Float, Float> secondPointLatLngAlt) {
        double minAltitude = Math.min(firstPointLatLngAlt.getValue2(), firstPointLatLngAlt.getValue2());

        Double dLat = deg2Rad(firstPointLatLngAlt.getValue0() - secondPointLatLngAlt.getValue0());
        Double dLon = deg2Rad(firstPointLatLngAlt.getValue1() - secondPointLatLngAlt.getValue1());

        Double angle = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(deg2Rad(firstPointLatLngAlt.getValue0())) * Math.cos(deg2Rad(secondPointLatLngAlt.getValue0())) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        Double c = 2 * Math.atan2(Math.sqrt(angle), Math.sqrt(1 - angle));

        double radius = earth_radius_in_meter;
        if (minAltitude >= 0) radius += minAltitude;
        double horizontalDistance = c * radius;
        if (minAltitude < 0) {
            return new Double(horizontalDistance).floatValue();
        } else {
            return new Double(Math.sqrt(Math.pow(horizontalDistance, 2) + Math.pow(Math.abs(firstPointLatLngAlt.getValue2() - firstPointLatLngAlt.getValue2()), 2))).floatValue();
        }
    }

    public static Optional<Triplet<Float, Float, Float>> getPosition(final String latitude, final String latitudeRef, final String longitude, final String longitudeRef, final String altitude) {
        if (StringUtils.isEmpty(latitude) || StringUtils.isEmpty(latitudeRef) || StringUtils.isEmpty(longitude) || StringUtils.isEmpty(longitudeRef)) {
            return Optional.absent();
        }
        final Float latitudeValue = new GPSLatitudeSensorRecord("uuid", LocalDateTime.now(), latitudeRef, latitude).getValue();
        final Float longitudeValue = new GPSLongitudeSensorRecord("uuid", LocalDateTime.now(), longitudeRef, longitude).getValue();
        float altitudeValue = 0;
        if (StringUtils.isNotEmpty(altitude) && StringUtils.containsIgnoreCase(altitude, " metres")) {
            altitudeValue = Float.parseFloat(StringUtils.remove(altitude, " metres"));
        }
        return Optional.of(Triplet.with(latitudeValue, longitudeValue, altitudeValue));
    }

    private static Double deg2Rad(Float degrees) {
        return new Double(degrees * (Math.PI / 180));
    }



}
