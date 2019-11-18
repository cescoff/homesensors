package com.desi.sensors.data.spreadsheet;

import com.desi.sensors.data.AggregatedSensorRecord;
import com.desi.sensors.data.Connector;
import com.desi.sensors.data.SensorNameProvider;
import com.desi.sensors.data.SensorRecord;
import com.desi.sensors.data.bean.DefaultAggregatedSensorRecord;
import com.desi.sensors.data.bean.TemperatureRecord;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.reflection.qual.GetMethod;
import org.javatuples.Pair;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;

public class SpreadSheetConverter implements Connector {

    private static final DateTimeZone SENSOR_TIMEZONE = DateTimeZone.forID("Europe/Paris");

    private static final DateTimeFormatter SPREADSHEET_FORMATTER = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm");

    private static final Logger logger = LoggerFactory.getLogger(SpreadSheetConverter.class);

    private static final String CHECKPOINT_URL = "https://us-central1-desisensors.cloudfunctions.net/SensorCheckpoint";

    private static final String PUSH_URL = "https://us-central1-desisensors.cloudfunctions.net/SensorReport";

    private static final DateTimeFormatter CHEKCPOINT_DATETIME_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    private static Iterable<AggregatedSensorRecord> aggregate(final Iterable<SensorRecord> records, int intervalInMinutes, final SensorNameProvider nameProvider) {
        if (Iterables.isEmpty(records)) {
            return Collections.emptyList();
        }
//        String sensorUUID = null;
        LocalDateTime minDate = null;
        LocalDateTime maxDate = null;
        for (final SensorRecord record : records) {
            if (minDate != null) {
                if (record.getDateTaken().isBefore(minDate)) {
                    minDate = record.getDateTaken();
                }
            } else {
                minDate = record.getDateTaken();
            }
            if (maxDate != null) {
                if (record.getDateTaken().isAfter(maxDate)) {
                    maxDate = record.getDateTaken();
                }
            } else {
                maxDate = record.getDateTaken();
            }
/*            if (sensorUUID != null) {
                if (!sensorUUID.equals(record.getSensorUUID())) {
                    throw new IllegalStateException("Cannot mix sensor ids while aggregating values");
                }
            } else {
                sensorUUID = record.getSensorUUID();
            }*/
        }

        final List<AggregatedSensorRecord> samples = Lists.newArrayList();
        LocalDateTime begin = minDate;
        LocalDateTime end = new LocalDateTime(begin.plusMinutes(intervalInMinutes));

//        samples.add(new DefaultAggregatedSensorRecord(begin, end, nameProvider));

        while (end.isBefore(maxDate)) {
            begin = end;
            end = new LocalDateTime(end.plusMinutes(intervalInMinutes));
            samples.add(new DefaultAggregatedSensorRecord(begin, end, nameProvider));
        }

        for (final SensorRecord record : records) {
            for (final AggregatedSensorRecord aggregatedSensorRecord : samples) {
                if (aggregatedSensorRecord.addValue(record)) {
                    break;
                }
            }
        }

        return ImmutableList.copyOf(samples);
    }

    private static float avg(final Iterable<SensorRecord> records) {
        if (Iterables.isEmpty(records)) {
//            System.out.println("EMPTY RECORDS");
            return 0.0f;
        }
        float count = 0.0f;
        for (final SensorRecord record : records) {
            count += record.getValue();
        }
        return new Double(new DecimalFormat("#.##").format(count / Iterables.size(records))).floatValue();
    }

    public boolean begin() {
        return true;
    }

    public boolean addRecords(final Iterable<SensorRecord> records, final SensorNameProvider sensorNameProvider) throws Exception {
        final Iterable<AggregatedSensorRecord> aggregatedSensorRecords = aggregate(records, 5, sensorNameProvider);

        final Map<String, String> sensorDisplayNames = getAllSensorUUIDsToNames(aggregatedSensorRecords);

        final Map<String, Float> minValues = Maps.newHashMap();
        for (final AggregatedSensorRecord record : aggregatedSensorRecords) {
            for (final String uuid : record.getSensorUUIDs()) {
                if (!minValues.containsKey(uuid)) {
                    minValues.put(uuid, record.getSensorValue(uuid));
                } else if (minValues.get(uuid) > record.getSensorValue(uuid)) {
                    minValues.put(uuid, record.getSensorValue(uuid));
                }
            }
        }

//        final StringBuilder csvOut = new StringBuilder("\"Date Time\",\"Heure du jour\"");

        final JSONObject jsonOutput = new JSONObject();
        final JSONArray jsonSensorDisplayNames = new JSONArray();
        for (final String uuid : sensorDisplayNames.keySet()) {
            final JSONObject jsonSensorDisplayName = new JSONObject();
            jsonSensorDisplayName.put("uuid", uuid);
            jsonSensorDisplayName.put("name", sensorDisplayNames.get(uuid));
            jsonSensorDisplayNames.put(jsonSensorDisplayName);
        }
        jsonOutput.put("SensorDisplayNames", jsonSensorDisplayNames);

/*        for (final String uuid : sensorDisplayNames.keySet()) {
            csvOut.append(",\"").append(sensorDisplayNames.get(uuid)).append("\",\"").append(sensorDisplayNames.get(uuid)).append("-Variation\"");
        }
        csvOut.append("\n");
*/
        if (Iterables.size(aggregatedSensorRecords) <= 1) {
            logger.info("No data to send");
            return true;
        }
        final JSONArray jsonSensorData = new JSONArray();
        for (final AggregatedSensorRecord record : aggregatedSensorRecords) {
/*            csvOut.append("\"").append(SPREADSHEET_FORMATTER.print(record.getPeriodBegin())).append("\",\"");
            csvOut.append(record.getPeriodBegin().getHourOfDay()).append("\"");
*/
            final JSONObject jsonSensorsRecords = new JSONObject();
            jsonSensorsRecords.put("DateTime", SPREADSHEET_FORMATTER.print(record.getPeriodBegin()));
            jsonSensorsRecords.put("HourOfDay", record.getPeriodBegin().getHourOfDay());

            final JSONArray jsonRecordsValues = new JSONArray();
            for (final String uuid : sensorDisplayNames.keySet()) {

                final float value = record.getSensorValue(uuid);
                final float valueVariation;
                if (value != 0) {
                    valueVariation = value - minValues.get(uuid);
                } else {
                    valueVariation = 0;
                }
//                csvOut.append(",\"").append(formatFloat(value)).append("\",\"").append(formatFloat(valueVariation)).append("\"");

                final JSONObject jsonRecordValue = new JSONObject();
                jsonRecordValue.put("UUID", uuid);
                jsonRecordValue.put("Value", formatFloat(value));
                jsonRecordsValues.put(jsonRecordValue);
            }
            jsonSensorsRecords.put("Sensors", jsonRecordsValues);

            jsonSensorData.put(jsonSensorsRecords);
//            csvOut.append("\n");
        }
        jsonSensorData.remove(jsonSensorData.length() - 1);
        jsonOutput.put("SensorData", jsonSensorData);

/*        final FileOutputStream fileOutputStreamCSV = new FileOutputStream(new File("Temperatures.csv"));
        fileOutputStreamCSV.write(csvOut.toString().getBytes());
        fileOutputStreamCSV.close();

        final FileOutputStream fileOutputStreamJSON = new FileOutputStream(new File("Temperatures.json"));
        fileOutputStreamJSON.write(jsonOutput.toString(1).getBytes());
        fileOutputStreamJSON.close();
*/

        final CloseableHttpClient client = HttpClients.createDefault();
        final HttpPost method = new HttpPost(PUSH_URL);
        method.setEntity(new StringEntity(jsonOutput.toString(), ContentType.APPLICATION_JSON));
//        method.setEntity(new UrlEncodedFormEntity(params));
        method.setHeader("Accept", "application/json");
        method.setHeader("Content-type", "application/json");
        final HttpResponse response;
        try {
            response = client.execute(method);
        } catch (IOException e) {
            logger.error("Failed to execute get method on URL '" + CHECKPOINT_URL + "'", e);
            return false;
        }
        if (response.getStatusLine().getStatusCode() != 200) {
            logger.error("Failed to push data status code is " + response.getStatusLine().getStatusCode() + " with message '" + response.getStatusLine().getReasonPhrase() + "' : " + IOUtils.toString(response.getEntity().getContent(), "UTF-8"));
            return false;
        }

        logger.info("Push response : " + IOUtils.toString(response.getEntity().getContent(), "UTF-8"));

        client.close();

        return true;
    }

    private String formatFloat(final float value) {
        DecimalFormat df = new DecimalFormat("#.#");
        return StringUtils.replace(df.format(new Float(value).doubleValue()), ".", ",");
    }

    private Map<String, String> getAllSensorUUIDsToNames(final Iterable<AggregatedSensorRecord> records) {
        final Map<String, String> result = new LinkedHashMap<String, String>();
        for (final AggregatedSensorRecord record : records) {
            for (final String uuid : record.getSensorUUIDs()) {
                if (!result.containsKey(uuid)) {
                    result.put(uuid, record.getDisplayName(uuid));
                }
            }
        }
        return result;
    }

    public Optional<LocalDateTime> getCheckPointValue(String sensorId) {
        final CloseableHttpClient client = HttpClients.createDefault();
        final HttpGet method = new HttpGet(CHECKPOINT_URL);
        final HttpResponse response;
        try {
            response = client.execute(method);
        } catch (IOException e) {
            logger.error("Failed to execute get method on URL '" + CHECKPOINT_URL + "'", e);
            return Optional.absent();
        }
        if (response.getStatusLine().getStatusCode() != 200) {
            logger.error("Failed to load checkpoint status code is " + response.getStatusLine().getStatusCode() + " with message '" + response.getStatusLine().getReasonPhrase() + "'");
            return Optional.absent();
        }
        final String valueStr;
        try {
            valueStr = StringUtils.remove(IOUtils.toString(response.getEntity().getContent(), "UTF-8"), "\"");
        } catch (IOException e) {
            logger.error("Failed to get response body on URL '" + CHECKPOINT_URL + "'", e);
            return Optional.absent();
        }
        try {
            client.close();
        } catch (IOException e) {
            logger.error("Failed to close HttpClient", e);
        }
        return Optional.of(CHEKCPOINT_DATETIME_FORMAT.parseLocalDateTime(valueStr));
    }

    public boolean end() {
        return true;
    }
}
