package com.desi.data.utils;

import com.desi.data.SensorRecord;

public class SensorRecordSQLHelper {

    public static String toSQLInsert(final SensorRecord sensorRecord, final String schema, final String tableName) {
        final StringBuilder result = new StringBuilder("INSERT INTO ");
        result.append(schema).append(".").append(tableName);
        result.append(" (uuid, value, dateTime) VALUES (\"");
        result.append(sensorRecord.getSensorUUID()).append("\", ").
                append(sensorRecord.getValue()).append(", ").
                append("\"").append(sensorRecord.getDateTaken().toString()).append("\");\n");
        return result.toString();
    }

}
