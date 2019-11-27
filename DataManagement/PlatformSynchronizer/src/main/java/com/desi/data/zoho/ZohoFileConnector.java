package com.desi.data.zoho;

import com.desi.data.Connector;
import com.desi.data.PlatformClientId;
import com.desi.data.SensorNameProvider;
import com.desi.data.SensorRecord;
import com.desi.data.config.PlatformCredentialsConfig;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.List;

public class ZohoFileConnector implements Connector {


    @Override
    public boolean begin(PlatformCredentialsConfig.Credentials credentials) {
        return true;
    }

    @Override
    public Optional<PlatformClientId> getPlatformId() {
        return Optional.absent();
    }

    @Override
    public boolean addRecords(Iterable<SensorRecord> records, SensorNameProvider nameProvider) throws Exception {
        final StringBuilder result = new StringBuilder();

        for (final SensorRecord record : records) {
            final List<String> newElements = Lists.newArrayList();
            newElements.add("\"" + replaceNames(record.getSensorUUID()) + "\"");
            newElements.add(DateTimeFormat.forPattern("dd-MMM-yyyy HH:mm:ss").print(record.getDateTaken()));
            newElements.add(formatFloat(record.getValue()));
            result.append(Joiner.on(",").join(newElements)).append("\n");
        }
        final FileOutputStream out = new FileOutputStream(new File("/Users/corentin/Documents/Developpement/Data", "data.csv"));
        IOUtils.write(result.toString(), out);
        out.close();
        return true;
    }

    private static String formatFloat(final float value) {
        DecimalFormat df = new DecimalFormat("#.#");
        return df.format(new Float(value).doubleValue());
    }

    private static String replaceNames(final String uuid) {
        String result = StringUtils.replace(uuid, "83ddd733-7ac4-4de2-a188-07b97977e9c0", "Livingroom");
        result = StringUtils.replace(result, "9ad754a1-4ae1-4a73-9658-734e19747617", "Outside");
        result = StringUtils.replace(result, "eef014ca-4261-40a8-aecd-ec41e466e5d0", "Bathroom");
        return result;
    }

    @Override
    public Optional<LocalDateTime> getCheckPointValue(String sensorId) {
        return Optional.of(new LocalDateTime("2019-11-21T11:40:01"));
    }

    @Override
    public boolean end() {
        return true;
    }
}
