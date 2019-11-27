package com.desi.data.impl;

import com.desi.data.SensorNameProvider;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;

public class StaticSensorNameProvider implements SensorNameProvider {

    private static Map<String, String> names = Maps.newHashMap();

    static {
        names.put("83ddd733-7ac4-4de2-a188-07b97977e9c0", "Bureau");
        names.put("9ad754a1-4ae1-4a73-9658-734e19747617", "Exterieur");
        names.put("eef014ca-4261-40a8-aecd-ec41e466e5d0", "Salle de bain");
    }


    public String getDisplayName(String uuid) {
        if (!names.containsKey(uuid)) {
            return uuid;
        }
        return names.get(uuid);
    }

    public Map<String, String> getDisplayNames(Iterable<String> uuids) {
        final ImmutableMap.Builder<String, String> result = ImmutableMap.builder();
        for (final String uuid : uuids) {
            if (!names.containsKey(uuid)) {
                result.put(uuid, uuid);
            } else {
                result.put(uuid, names.get(uuid));
            }
        }
        return result.build();
    }
}
