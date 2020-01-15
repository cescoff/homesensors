package com.desi.data.utils;

import org.javatuples.Triplet;

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

    private static Double deg2Rad(Float degrees) {
        return new Double(degrees * (Math.PI / 180));
    }

}
