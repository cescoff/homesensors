package com.desi.data;

import com.desi.data.config.PlatformCredentialsConfig;
import com.google.common.base.Optional;
import org.joda.time.LocalDateTime;

import java.io.File;
import java.util.Map;

public interface DataSource {

    public Optional<PlatformClientId> getPlatformId();

    public boolean begin(PlatformCredentialsConfig.Credentials credentials, final File configDir);

    public Iterable<String> getSensorsUUIDs();

    public Map<String, SensorRecord> getLastSensorsValues();

    public Iterable<SensorRecord> getRecords(final String uuid, final LocalDateTime checkPoint);

    public boolean end();


}
