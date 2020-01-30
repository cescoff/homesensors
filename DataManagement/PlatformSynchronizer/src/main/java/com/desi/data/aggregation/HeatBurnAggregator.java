package com.desi.data.aggregation;

import com.desi.data.SensorNameProvider;
import com.desi.data.SensorRecord;
import com.desi.data.SensorType;
import com.desi.data.SensorUnit;
import com.desi.data.bean.HeatBurnSensorRecord;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class HeatBurnAggregator {

    private static final Logger logger = LoggerFactory.getLogger(HeatBurnAggregator.class);

    private static final float HEATING_THRESHOLD = 0.1f;
    private static final float HEATER_THRESHOLD = 0.2f;


    private final LocalDateTime start;

    private final LocalDateTime end;

    private final Map<String, Iterable<SensorRecord>> recordsBySensor = Maps.newHashMap();

    private final SensorNameProvider sensorNameProvider;

    public HeatBurnAggregator(LocalDateTime start, LocalDateTime end, SensorNameProvider sensorNameProvider) {
        this.start = start;
        this.end = end;
        this.sensorNameProvider = sensorNameProvider;
    }

    public boolean add(final SensorRecord sensorRecord) {
        if (start.minusMinutes(60).isBefore(sensorRecord.getDateTaken()) && sensorRecord.getDateTaken().isBefore(end)) {
            if (sensorNameProvider.getType(sensorRecord.getSensorUUID()) == SensorType.HEATING_TEMPERATURE
                || sensorNameProvider.getType(sensorRecord.getSensorUUID()) == SensorType.HEATER_TEMPERATURE) {
                if (!recordsBySensor.containsKey(sensorRecord.getSensorUUID())) {
                    recordsBySensor.put(sensorRecord.getSensorUUID(), Lists.newArrayList());
                }
                ((List<SensorRecord>) recordsBySensor.get(sensorRecord.getSensorUUID())).add(sensorRecord);
                return true;
            }
        }
        return false;
    }

    public Map<String, Iterable<SensorRecord>> compute() {
        final Map<String, Iterable<SensorRecord>> result = Maps.newHashMap();
        for (final String sensorUUID : this.recordsBySensor.keySet()) {
            logger.info("Computing value for sensor " + this.sensorNameProvider.getDisplayName(sensorUUID) + "[" + sensorUUID + "]");
            if (Iterables.size(this.recordsBySensor.get(sensorUUID)) <= 4) {
                logger.warn(this.sensorNameProvider.getDisplayName(sensorUUID) + "[" + sensorUUID + "] too few value count for computation");
            } else {
                final List<SensorRecord> sorted = Ordering.natural().onResultOf(DATE_SORT).sortedCopy(this.recordsBySensor.get(sensorUUID));
                if (sorted.get(0).getDateTaken().isAfter(start.minusMinutes(45))) {
                    logger.warn(this.sensorNameProvider.getDisplayName(sensorUUID) + "[" + sensorUUID + "] computation requires values from at least 45 minutes before start");
                } else {
                    final SensorType sensorType = this.sensorNameProvider.getType(sensorUUID);
                    List<SensorRecord> records = Lists.newArrayList();

                    List<SensorRecord> currentBurn = Lists.newArrayList();

                    for (int index = 1; index < sorted.size(); index++) {
                        float valueCandidate = sorted.get(index).getValue() - sorted.get(index - 1).getValue();
                        float nextValueCandidate = -1;
                        if (index < (sorted.size() - 1)) {
                            nextValueCandidate = sorted.get(index + 1).getValue() - sorted.get(index).getValue();
                        }
                        if (valueCandidate > 0) {
                            if (currentBurn.size() == 0) {
                                logger.info(this.sensorNameProvider.getDisplayName(sensorUUID) + "[" + sensorUUID + "] Burn start detected at " + sorted.get(index - 1).getDateTaken());
                            }
                            currentBurn.add(sorted.get(index - 1));
/*                            if (nextValueCandidate == 0) {
                                currentBurn.add(sorted.get(index));
                            }*/
/*                            if (index == (sorted.size() - 1)) {
                                currentBurn.add(sorted.get(sorted.size() - 1));
                            }*/
                        } else if (currentBurn.size() > 0 &&
                                ((valueCandidate < 0 && nextValueCandidate < 0)
                                    || index == (sorted.size() - 1))) {
                            //currentBurn.add(sorted.get(index - 1));
                            logger.info(this.sensorNameProvider.getDisplayName(sensorUUID) + "[" + sensorUUID + "] Burn end detected at " + sorted.get(index - 1).getDateTaken());
                            float minValue = currentBurn.get(0).getValue();
                            for (int burnIndex = 0; burnIndex<(currentBurn.size() - 1); burnIndex++) {
                                float delta = currentBurn.get(burnIndex).getValue() - minValue;
                                if (delta > 0) {
                                    if (start.isBefore(currentBurn.get(burnIndex).getDateTaken()) && currentBurn.get(burnIndex).getDateTaken().isBefore(end)) {
                                        float time = ((currentBurn.get(burnIndex + 1).getDateTaken().toDate().getTime() - currentBurn.get(burnIndex).getDateTaken().toDate().getTime()) / 1000);
                                        final float value = delta * time + ((currentBurn.get(burnIndex + 1).getValue() - currentBurn.get(burnIndex).getValue()) / 2) * time;
                                        final LocalDateTime dateTaken = currentBurn.get(burnIndex).getDateTaken();
                                        records.add(generateRecord(this.sensorNameProvider.getBurnerUUID("corentin.escoffier@gmail.com"), dateTaken, value));
                                    }
                                }
                            }
                            currentBurn = Lists.newArrayList();
                        }
                    }
                    result.put(sensorUUID, records);
                }
            }
        }
        return result;
    }

    private SensorRecord generateRecord(final String uuid, final LocalDateTime dateTime, final float value) {
        return new SensorRecord() {
            @Override
            public LocalDateTime getDateTaken() {
                return dateTime;
            }

            @Override
            public float getValue() {
                return value;
            }

            @Override
            public String getSensorUUID() {
                return uuid;
            }

            @Override
            public SensorUnit getUnit() {
                return SensorUnit.CUBIC_METER;
            }

            @Override
            public SensorType getType() {
                return SensorType.GAS_VOLUME_ODOMETER;
            }
        };
    }

    private float getHeatingThreshold(final SensorType sensorType) {
        if (sensorType == SensorType.HEATING_TEMPERATURE) {
            return HEATING_THRESHOLD;
        } else if (sensorType == SensorType.HEATER_TEMPERATURE) {
            return HEATER_THRESHOLD;
        }
        return 1000;
    }

    private static final Function<SensorRecord, LocalDateTime> DATE_SORT = new Function<SensorRecord, LocalDateTime>() {
        @Override
        public LocalDateTime apply(SensorRecord sensorRecord) {
            return sensorRecord.getDateTaken();
        }
    };

}
