package com.desi.data;

import java.util.Map;

public interface SensorNameProvider {

    public String getDisplayName(final String uuid);

    public Map<String, String> getDisplayNames(final Iterable<String> uuids);

    public SensorType getType(final String uuid);

    public SensorUnit getUnit(final String uuid);

    public String getBurnerUUID(final String ownerEmail);

    public AggregationType getAggregationType(final String uuid);

}
