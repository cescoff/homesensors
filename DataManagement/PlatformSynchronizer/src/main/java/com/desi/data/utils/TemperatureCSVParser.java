package com.desi.data.utils;

import com.desi.data.bean.TemperatureRecord;
import com.desi.data.impl.StaticSensorNameProvider;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.InputStream;
import java.io.InputStreamReader;

public class TemperatureCSVParser {

    private static final DateTimeFormatter CSV_FORMATTER_1 = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter CSV_FORMATTER_2 = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss");

    public static Iterable<TemperatureRecord> parseContent(final InputStream content) throws Exception {

        final InputStreamReader inputStreamReader = new InputStreamReader(content);

        final LineIterator lineIterator = new LineIterator(inputStreamReader);

        final ImmutableList.Builder<TemperatureRecord> result = ImmutableList.builder();

        while (lineIterator.hasNext()) {
            final String line = lineIterator.nextLine();
            final String[] elements = StringUtils.split(line, ";");
            if (elements.length >= 4) {
                final String sensorUUID = StringUtils.trim(elements[0]);
                final String dateTimeString = elements[1] + " " + elements[2];
                final float value = new Double(StringUtils.remove(elements[3], "C=")).floatValue();

                if (value != 85.0) {
                    result.add(new TemperatureRecord(parseDateTime(dateTimeString), value, sensorUUID, new StaticSensorNameProvider().getType(sensorUUID)));
                }
            } else {
                throw new Exception("UNPARSABLE LINE '" + line + "'");
            }
        }

        content.close();

        return result.build();
    }

    private static LocalDateTime parseDateTime(final String dateTimeString) {
        try {
            return CSV_FORMATTER_1.parseDateTime(dateTimeString).toLocalDateTime();
        } catch (Throwable t) {
            return CSV_FORMATTER_2.parseDateTime(dateTimeString).toLocalDateTime();
        }
    }

}
