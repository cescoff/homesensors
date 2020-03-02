package com.desi.data.impl;

import com.desi.data.AggregationType;
import com.desi.data.SensorNameProvider;
import com.desi.data.SensorType;
import com.desi.data.SensorUnit;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;

public class StaticSensorNameProvider implements SensorNameProvider {



    private static Map<String, String> names = Maps.newHashMap();
    private static Map<String, SensorType> types = Maps.newHashMap();
    private static Map<String, SensorUnit> units = Maps.newHashMap();
    private static Map<String, String> burnersUUIDs = Maps.newHashMap();



    static {
        names.put("83ddd733-7ac4-4de2-a188-07b97977e9c0", "Bureau");
        names.put("9ad754a1-4ae1-4a73-9658-734e19747617", "Exterieur");
        names.put("eef014ca-4261-40a8-aecd-ec41e466e5d0", "Salle de bain");
        names.put("c6f07270-c345-4e6d-9098-2c758efcc13f", "Retour chauffage");
        names.put("5f7cee1f-2e74-48b4-a53a-9be4bbe0abec", "Chauffage");
        names.put("c4944883-1151-4263-9e7b-965e285e212c", "Eau chaude");
        names.put("7012f31e-78e9-4b41-9d0a-6573108a144f", "Burner");

        types.put("83ddd733-7ac4-4de2-a188-07b97977e9c0", SensorType.INDOOR_TEMPERATURE);
        types.put("9ad754a1-4ae1-4a73-9658-734e19747617", SensorType.OUTDOOR_TEMPERATURE);
        types.put("eef014ca-4261-40a8-aecd-ec41e466e5d0", SensorType.INDOOR_TEMPERATURE);
        types.put("c6f07270-c345-4e6d-9098-2c758efcc13f", SensorType.INDOOR_TEMPERATURE);
        types.put("5f7cee1f-2e74-48b4-a53a-9be4bbe0abec", SensorType.HEATING_TEMPERATURE);
        types.put("c4944883-1151-4263-9e7b-965e285e212c", SensorType.HEATER_TEMPERATURE);
        types.put("7012f31e-78e9-4b41-9d0a-6573108a144f", SensorType.GAS_VOLUME_ODOMETER);

        units.put("83ddd733-7ac4-4de2-a188-07b97977e9c0", SensorUnit.DEGREES_CELSIUS);
        units.put("9ad754a1-4ae1-4a73-9658-734e19747617", SensorUnit.DEGREES_CELSIUS);
        units.put("eef014ca-4261-40a8-aecd-ec41e466e5d0", SensorUnit.DEGREES_CELSIUS);
        units.put("c6f07270-c345-4e6d-9098-2c758efcc13f", SensorUnit.DEGREES_CELSIUS);
        units.put("5f7cee1f-2e74-48b4-a53a-9be4bbe0abec", SensorUnit.DEGREES_CELSIUS);
        units.put("c4944883-1151-4263-9e7b-965e285e212c", SensorUnit.DEGREES_CELSIUS);
        units.put("7012f31e-78e9-4b41-9d0a-6573108a144f", SensorUnit.SECONDS);

        burnersUUIDs.put("corentin.escoffier@gmail.com", "7012f31e-78e9-4b41-9d0a-6573108a144f");
    }

    @Override
    public String getBurnerUUID(String ownerEmail) {
        return burnersUUIDs.get(ownerEmail);
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

    @Override
    public SensorType getType(String uuid) {
        return types.get(uuid);
    }

    @Override
    public SensorUnit getUnit(String uuid) {
        return units.get(uuid);
    }

    @Override
    public AggregationType getAggregationType(String uuid) {
        if (getType(uuid) == null) {
            return AggregationType.AVG;
        }
        if (getType(uuid) == SensorType.GAS_VOLUME_ODOMETER) {
            return AggregationType.SUM;
        }
        return AggregationType.AVG;
    }
}
