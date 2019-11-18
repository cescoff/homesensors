package com.desi.data.bean;

import com.desi.data.AggregatedSensorRecord;
import com.desi.data.SensorNameProvider;
import com.desi.data.SensorRecord;
import com.desi.data.SensorUnit;
import com.google.common.collect.*;
import org.joda.time.LocalDateTime;

import java.util.List;
import java.util.Map;

public class DefaultAggregatedSensorRecord implements AggregatedSensorRecord {

    private final LocalDateTime begin;

    private final LocalDateTime end;

    private final SensorNameProvider sensorNameProvider;

    private final Map<String, List<SensorRecord>> recordsByUUID = Maps.newHashMap();

    private final Map<String, SensorUnit> units = Maps.newHashMap();

    public DefaultAggregatedSensorRecord(LocalDateTime begin, LocalDateTime end, SensorNameProvider sensorNameProvider) {
        this.begin = begin;
        this.end = end;
        this.sensorNameProvider = sensorNameProvider;
    }

    public LocalDateTime getPeriodBegin() {
        return begin;
    }

    public LocalDateTime getPeriodEnd() {
        return end;
    }

    public Iterable<String> getSensorUUIDs() {
        return ImmutableList.copyOf(Ordering.natural().sortedCopy(recordsByUUID.keySet()));
    }

    public String getDisplayName(String uuid) {
        return sensorNameProvider.getDisplayName(uuid);
    }

    public SensorUnit getUnit(String uuid) {
        return units.get(uuid);
    }

    public float getSensorValue(String uuid) {
        if (recordsByUUID.containsKey(uuid)) {
            float sum = 0;
            for (final SensorRecord record : recordsByUUID.get(uuid)) {
                sum+=record.getValue();
            }
            return sum / Iterables.size(recordsByUUID.get(uuid));
        }
        return 0;
    }

    public boolean addValue(SensorRecord record) {
        if (record.getDateTaken().isBefore(begin) || record.getDateTaken().isAfter(end) || record.getDateTaken().equals(end)) {
            return false;
        }

        if (!recordsByUUID.containsKey(record.getSensorUUID())) {
            recordsByUUID.put(record.getSensorUUID(), Lists.<SensorRecord>newArrayList());
            units.put(record.getSensorUUID(), record.getUnit());
        }

        recordsByUUID.get(record.getSensorUUID()).add(record);

        return true;
    }
}
