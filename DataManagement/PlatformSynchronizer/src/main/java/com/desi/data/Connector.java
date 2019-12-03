package com.desi.data;

import com.desi.data.config.PlatformCredentialsConfig;
import com.google.common.base.Optional;
import org.joda.time.LocalDateTime;

import java.io.File;

public interface Connector {

    public Optional<PlatformClientId> getPlatformId();

    public boolean begin(PlatformCredentialsConfig.Credentials credentials, final File configDir);

    public boolean addRecords(final Iterable<SensorRecord> records, final SensorNameProvider nameProvider) throws Exception;

    public Optional<LocalDateTime> getCheckPointValue(final String sensorId);

    public boolean end();

}
