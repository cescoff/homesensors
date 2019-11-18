package com.desi.data;

import com.google.common.base.Optional;
import org.joda.time.LocalDateTime;

public interface Connector {

    public boolean begin();

    public boolean addRecords(final Iterable<SensorRecord> records, final SensorNameProvider nameProvider) throws Exception;

    public Optional<LocalDateTime> getCheckPointValue(final String sensorId);

    public boolean end();

}
