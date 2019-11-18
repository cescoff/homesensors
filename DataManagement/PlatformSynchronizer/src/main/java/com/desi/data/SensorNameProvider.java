package com.desi.data;

import java.util.Map;

public interface SensorNameProvider {

    public String getDisplayName(final String uuid);

    public Map<String, String> getDisplayNames(final Iterable<String> uuids);

}
