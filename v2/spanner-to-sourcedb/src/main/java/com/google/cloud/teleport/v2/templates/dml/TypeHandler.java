package com.google.cloud.teleport.v2.templates.dml;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnDefinition;
import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

class TypeHandler {
    private final CqlSession session;

    public TypeHandler(CqlSession session) {
        this.session = session;
    }

    public ByteBuffer handleCassandraBoolType(String colName, JSONObject valuesJson) {
        boolean value = valuesJson.getBoolean(colName);
        return session.getContext().getCodecRegistry().codecFor(DataTypes.BOOLEAN).encode(value, session.getContext().getProtocolVersion());
    }

    public ByteBuffer handleCassandraFloatType(String colName, JSONObject valuesJson) {
        Float colValue = valuesJson.getBigDecimal(colName).floatValue();
        return session.getContext().getCodecRegistry().codecFor(DataTypes.FLOAT).encode(colValue, session.getContext().getProtocolVersion());
    }

    public ByteBuffer handleCassandraDoubleType(String colName, JSONObject valuesJson) {
        double colValue = valuesJson.getBigDecimal(colName).floatValue();
        return session.getContext().getCodecRegistry().codecFor(DataTypes.FLOAT).encode(colValue, session.getContext().getProtocolVersion());
    }

    public ByteBuffer handleCassandraBlobType(String colName, JSONObject valuesJson) {
        Object colValue = valuesJson.opt(colName);
        if (colValue == null) {
            return null;
        }

        byte[] byteArray;

        if (colValue instanceof byte[]) {
            byteArray = (byte[]) colValue;
        } else if (colValue instanceof String) {
            byteArray = java.util.Base64.getDecoder().decode((String) colValue);
        }
        // Handle invalid data types
        else {
            return null;
        }

        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);

        return session.getContext()
                .getCodecRegistry()
                .codecFor(DataTypes.BLOB)
                .encode(byteBuffer, session.getContext().getProtocolVersion());
    }

    public ByteBuffer handleCassandraDateType(String colName, JSONObject valuesJson) {
        Object colValue = valuesJson.opt(colName);
        if (colValue == null) {
            return null;
        }

        java.util.Date date;

        if (colValue instanceof String) {
            try {
                date = new SimpleDateFormat("yyyy-MM-dd").parse((String) colValue);
            } catch (ParseException e) {
                throw new IllegalArgumentException("Invalid date format for column " + colName, e);
            }
        } else if (colValue instanceof java.util.Date) {
            date = (java.util.Date) colValue;
        } else {
            throw new IllegalArgumentException("Unsupported type for column " + colName);
        }

        long millisSinceEpoch = date.getTime();
        int daysSinceEpoch = (int) (millisSinceEpoch / (24 * 60 * 60 * 1000L));

        return session.getContext()
                .getCodecRegistry()
                .codecFor(DataTypes.DATE)
                .encode(daysSinceEpoch, session.getContext().getProtocolVersion());


    }

    public ByteBuffer handleCassandraTimestampType(String colName, JSONObject valuesJson) {
        Object colValue = valuesJson.opt(colName);
        if (colValue == null) {
            return null;
        }

        java.util.Date date;

        if (colValue instanceof String) {
            try {
                date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").parse((String) colValue);
            } catch (ParseException e) {
                throw new IllegalArgumentException("Invalid timestamp format for column " + colName, e);
            }
        } else if (colValue instanceof java.util.Date) {
            date = (java.util.Date) colValue;
        } else if (colValue instanceof Long) {
            date = new java.util.Date((Long) colValue);
        } else {
            throw new IllegalArgumentException("Unsupported type for column " + colName);
        }

        long millisSinceEpoch = date.getTime();

        return session.getContext()
                .getCodecRegistry()
                .codecFor(DataTypes.TIMESTAMP)
                .encode(millisSinceEpoch, session.getContext().getProtocolVersion());
    }

    public ByteBuffer handleCassandraTextType(String colName, JSONObject valuesJson) {
        String value = valuesJson.optString(colName, null); // Get the value or null if the key is not found or the value is null

        if (value == null) {
            return null;
        }

        return session.getContext()
                .getCodecRegistry()
                .codecFor(DataTypes.TEXT)
                .encode(value, session.getContext().getProtocolVersion());

    }

    public ByteBuffer handleCassandraUuidType(String colName, JSONObject valuesJson) {
        String uuidString = valuesJson.optString(colName, null); // Get the value or null if the key is not found or the value is null

        if (uuidString == null) {
            return null;
        }

        UUID uuid = UUID.fromString(uuidString);
        return session.getContext()
                .getCodecRegistry()
                .codecFor(DataTypes.UUID)
                .encode(uuid, session.getContext().getProtocolVersion());

    }

    public ByteBuffer handleCassandraBigintType(String colName, JSONObject valuesJson) {
        long colValue = valuesJson.getBigInteger(colName).longValue();
        return session.getContext().getCodecRegistry().codecFor(DataTypes.BIGINT).encode(colValue, session.getContext().getProtocolVersion());
    }

    public ByteBuffer handleCassandraIntType(String colName, JSONObject valuesJson) {
        int colValue = valuesJson.getBigInteger(colName).intValue();
        return session.getContext().getCodecRegistry().codecFor(DataTypes.INT).encode(colValue, session.getContext().getProtocolVersion());
    }


    public ByteBuffer handleCassandraWriteTimeFuncType(String colName, JSONObject valuesJson) {
        long timestampMillis = valuesJson.optLong(colName, -1L);

        if (timestampMillis == -1L) {
            return null;
        }
        return session.getContext().getCodecRegistry().codecFor(DataTypes.BIGINT).encode(timestampMillis, session.getContext().getProtocolVersion());
    }

    public ByteBuffer handleInt64ArrayType(String colName, JSONObject valuesJson) {
        JSONArray jsonArray = valuesJson.getJSONArray(colName);
        List<Long> colValueList = new ArrayList<>();

        // Convert each element to Long and add it to the list
        for (int i = 0; i < jsonArray.length(); i++) {
            colValueList.add(jsonArray.getLong(i));
        }

        return session.getContext().getCodecRegistry().codecFor(DataTypes.listOf(DataTypes.BIGINT)).encode(colValueList, session.getContext().getProtocolVersion());
    }

    public ByteBuffer handleInt64SetType(String colName, JSONObject valuesJson) {
        JSONArray jsonArray = valuesJson.getJSONArray(colName);
        Set<Long> colValueSet = new HashSet<>();

        // Convert each element to Long and add it to the set
        for (int i = 0; i < jsonArray.length(); i++) {
            colValueSet.add(jsonArray.getLong(i));
        }
        return session.getContext().getCodecRegistry().codecFor(DataTypes.setOf(DataTypes.BIGINT)).encode(colValueSet, session.getContext().getProtocolVersion());
    }

    // Handler for ARRAY<Int32> (also serves as Set<Int32>)
    public ByteBuffer handleInt64ArrayAsInt32Array(String colName, JSONObject valuesJson) {
        JSONArray jsonArray = valuesJson.getJSONArray(colName);
        List<Long> colValueList = new ArrayList<>();

        // Convert each element to Long and add it to the list
        for (int i = 0; i < jsonArray.length(); i++) {
            colValueList.add(jsonArray.getLong(i));
        }
        List<Integer> col32 = colValueList.stream().map(Long::intValue).collect(Collectors.toList());
        return session.getContext().getCodecRegistry().codecFor(DataTypes.listOf(DataTypes.INT)).encode(col32, session.getContext().getProtocolVersion());
    }

    public ByteBuffer handleInt64ArrayAsInt32Set(String colName, JSONObject valuesJson) {
        JSONArray jsonArray = valuesJson.getJSONArray(colName);
        Set<Long> colValueSet = new HashSet<>();

        // Convert each element to Long and add it to the set
        for (int i = 0; i < jsonArray.length(); i++) {
            colValueSet.add(jsonArray.getLong(i));
        }
        Set<Integer> col32 = colValueSet.stream().map(Long::intValue).collect(Collectors.toSet());
        return session.getContext().getCodecRegistry().codecFor(DataTypes.setOf(DataTypes.INT)).encode(col32, session.getContext().getProtocolVersion());
    }

    // Handler for ARRAY<String> (also serves as Set<String>)
    public ByteBuffer handleStringArrayType(String colName, JSONObject valuesJson) {
        JSONArray jsonArray = valuesJson.getJSONArray(colName);
        List<String> colValues = new ArrayList<>();

        // Convert each element to String and add it to the list
        for (int i = 0; i < jsonArray.length(); i++) {
            colValues.add(jsonArray.getString(i));
        }

        return session.getContext().getCodecRegistry().codecFor(DataTypes.listOf(DataTypes.TEXT)).encode(colValues, session.getContext().getProtocolVersion());
    }

    public ByteBuffer handleStringSetType(String colName, JSONObject valuesJson) {
        JSONArray jsonArray = valuesJson.getJSONArray(colName);
        Set<String> colValues = new HashSet<>();

        // Convert each element to String and add it to the list
        for (int i = 0; i < jsonArray.length(); i++) {
            colValues.add(jsonArray.getString(i));
        }
        return session.getContext().getCodecRegistry().codecFor(DataTypes.setOf(DataTypes.TEXT)).encode(colValues, session.getContext().getProtocolVersion());
    }

    // Handler for ARRAY<Boolean> (also serves as Set<Boolean>)
    public ByteBuffer handleBoolArrayType(String colName, JSONObject valuesJson) {
        JSONArray jsonArray = valuesJson.getJSONArray(colName);
        List<Boolean> colValues = new ArrayList<>();

        // Convert each element to String and add it to the list
        for (int i = 0; i < jsonArray.length(); i++) {
            colValues.add(jsonArray.getBoolean(i));
        }
        return session.getContext().getCodecRegistry().codecFor(DataTypes.listOf(DataTypes.BOOLEAN)).encode(colValues, session.getContext().getProtocolVersion());
    }

    public ByteBuffer handleBoolSetTypeString(String colName, JSONObject valuesJson) {
        JSONArray jsonArray = valuesJson.getJSONArray(colName);
        Set<Boolean> colValues = new HashSet<>();

        // Convert each element to String and add it to the list
        for (int i = 0; i < jsonArray.length(); i++) {
            colValues.add(jsonArray.getBoolean(i));
        }
        return session.getContext().getCodecRegistry().codecFor(DataTypes.setOf(DataTypes.BOOLEAN)).encode(colValues, session.getContext().getProtocolVersion());
    }

    // Handler for ARRAY<Double> (float64 in Go, also serves as Set<Double>)
    public ByteBuffer handleFloat64ArrayType(String colName, JSONObject valuesJson) {
        JSONArray jsonArray = valuesJson.getJSONArray(colName);
        List<Double> colValues = new ArrayList<>();

        // Convert each element to String and add it to the list
        for (int i = 0; i < jsonArray.length(); i++) {
            colValues.add(jsonArray.getDouble(i));
        }
        return session.getContext().getCodecRegistry().codecFor(DataTypes.listOf(DataTypes.DOUBLE)).encode(colValues, session.getContext().getProtocolVersion());
    }

    public ByteBuffer handleFloat64SetType(String colName, JSONObject valuesJson) {
        JSONArray jsonArray = valuesJson.getJSONArray(colName);
        Set<Double> colValues = new HashSet<>();

        // Convert each element to String and add it to the list
        for (int i = 0; i < jsonArray.length(); i++) {
            colValues.add(jsonArray.getDouble(i));
        }
        return session.getContext().getCodecRegistry().codecFor(DataTypes.setOf(DataTypes.DOUBLE)).encode(colValues, session.getContext().getProtocolVersion());
    }

    // Handler for ARRAY<Float> (float32 in other contexts, also serves as Set<Float>)
    public ByteBuffer handleFloatArrayType(String colName, JSONObject valuesJson) {
        JSONArray jsonArray = valuesJson.getJSONArray(colName);
        List<Double> colValues = new ArrayList<>();

        // Convert each element to String and add it to the list
        for (int i = 0; i < jsonArray.length(); i++) {
            colValues.add(jsonArray.getDouble(i));
        }
        List<Float> col32 = colValues.stream().map(Double::floatValue).collect(Collectors.toList());
        return session.getContext().getCodecRegistry().codecFor(DataTypes.listOf(DataTypes.FLOAT)).encode(col32, session.getContext().getProtocolVersion());
    }

    public ByteBuffer handleFloatSetType(String colName, JSONObject valuesJson) {
        JSONArray jsonArray = valuesJson.getJSONArray(colName);
        Set<Double> colValues = new HashSet<>();

        // Convert each element to String and add it to the list
        for (int i = 0; i < jsonArray.length(); i++) {
            colValues.add(jsonArray.getDouble(i));
        }
        Set<Float> col32 = colValues.stream().map(Double::floatValue).collect(Collectors.toSet());
        return session.getContext().getCodecRegistry().codecFor(DataTypes.setOf(DataTypes.FLOAT)).encode(col32, session.getContext().getProtocolVersion());
    }

    // Handler for ARRAY<Date> (also serves as Set<Date>)
    public ByteBuffer handleDateArrayType(String colName, JSONObject valuesJson) {
        JSONArray jsonArray = valuesJson.getJSONArray(colName);
        List<Integer> cqlDates = new ArrayList<>();

        // Convert each date to the appropriate CQL date format (days since epoch)
        for (int i = 0; i < jsonArray.length(); i++) {
            String dateString = jsonArray.getString(i);  // Assuming the date is in String format, modify if needed
            com.google.cloud.Date googleDate = com.google.cloud.Date.parseDate(dateString);  // Parse string to com.google.cloud.Date
            cqlDates.add(convertToCQLDate(googleDate));  // Convert to CQL date (days since epoch)
        }

        return session.getContext()
                .getCodecRegistry()
                .codecFor(DataTypes.listOf(DataTypes.DATE))
                .encode(cqlDates, session.getContext().getProtocolVersion());
    }

    public ByteBuffer handleDateSetType(String colName, JSONObject valuesJson) {
        JSONArray jsonArray = valuesJson.getJSONArray(colName);
        Set<Integer> cqlDates = new HashSet<>();

        // Convert each date to the appropriate CQL date format (days since epoch)
        for (int i = 0; i < jsonArray.length(); i++) {
            String dateString = jsonArray.getString(i);
            com.google.cloud.Date googleDate = com.google.cloud.Date.parseDate(dateString);
            cqlDates.add(convertToCQLDate(googleDate));
        }
        return session.getContext().getCodecRegistry().codecFor(DataTypes.setOf(DataTypes.DATE)).encode(cqlDates, session.getContext().getProtocolVersion());
    }

    // Handler for ARRAY<Timestamp> (also serves as Set<Timestamp>)
    public ByteBuffer handleTimestampArrayType(String colName, JSONObject valuesJson) {
        JSONArray jsonArray = valuesJson.getJSONArray(colName);
        List<Timestamp> timestampList = new ArrayList<>();

        for (int i = 0; i < jsonArray.length(); i++) {
            String timestampString = jsonArray.getString(i);  // Assuming timestamps are stored as Strings
            try {
                Timestamp timestamp = Timestamp.valueOf(timestampString);  // Convert String to Timestamp
                timestampList.add(timestamp);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("Invalid timestamp format in JSON for column " + colName, e);
            }
        }

        return session.getContext()
                .getCodecRegistry()
                .codecFor(DataTypes.listOf(DataTypes.TIMESTAMP))
                .encode(timestampList, session.getContext().getProtocolVersion());
    }

    public ByteBuffer handleTimestampSetType(String colName, JSONObject valuesJson) {
        JSONArray jsonArray = valuesJson.getJSONArray(colName);
        Set<Timestamp> timestampList = new HashSet<>();

        for (int i = 0; i < jsonArray.length(); i++) {
            String timestampString = jsonArray.getString(i);  // Assuming timestamps are stored as Strings
            try {
                Timestamp timestamp = Timestamp.valueOf(timestampString);  // Convert String to Timestamp
                timestampList.add(timestamp);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("Invalid timestamp format in JSON for column " + colName, e);
            }
        }

        return session.getContext()
                .getCodecRegistry()
                .codecFor(DataTypes.listOf(DataTypes.TIMESTAMP))
                .encode(timestampList, session.getContext().getProtocolVersion());
    }

    // Handler for ARRAY<Blob> (also serves as Set<Blob>)
    public ByteBuffer handleByteArrayType(String colName, JSONObject valuesJson) {
        JSONArray jsonArray = valuesJson.getJSONArray(colName);
        List<ByteBuffer> byteBufferList = new ArrayList<>();

        for (int i = 0; i < jsonArray.length(); i++) {
            try {
                byte[] bytes = Base64.getDecoder().decode(jsonArray.getString(i)); // Assuming data is Base64-encoded
                byteBufferList.add(ByteBuffer.wrap(bytes));
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("Invalid byte array format for column " + colName, e);
            }
        }

        return session.getContext()
                .getCodecRegistry()
                .codecFor(DataTypes.listOf(DataTypes.BLOB))
                .encode(byteBufferList, session.getContext().getProtocolVersion());
    }

    public ByteBuffer handleByteSetType(String colName, JSONObject valuesJson) {
        JSONArray jsonArray = valuesJson.getJSONArray(colName);
        Set<ByteBuffer> byteBufferList = new HashSet<>();

        for (int i = 0; i < jsonArray.length(); i++) {
            try {
                byte[] bytes = Base64.getDecoder().decode(jsonArray.getString(i));
                byteBufferList.add(ByteBuffer.wrap(bytes));
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("Invalid byte array format for column " + colName, e);
            }
        }

        return session.getContext()
                .getCodecRegistry()
                .codecFor(DataTypes.listOf(DataTypes.BLOB))
                .encode(byteBufferList, session.getContext().getProtocolVersion());
    }

    public static String getColumnValueByType(
            SpannerColumnDefinition spannerColDef,
            SourceColumnDefinition sourceColDef,
            JSONObject valuesJson,
            String sourceDbTimezoneOffset
    ) {

        String columnType = sourceColDef.getType().getName();
        ByteBuffer colValue = null;
        String colType = spannerColDef.getType().getName();
        String colName = spannerColDef.getName();

//        if ("FLOAT64".equals(colType)) {
//            colValue = valuesJson.getBigDecimal(colName).toString();
//        } else if ("BOOL".equals(colType)) {
//            colValue = (new Boolean(valuesJson.getBoolean(colName))).toString();
//        } else if ("STRING".equals(colType) && spannerColDef.getType().getIsArray()) {
//            colValue =
//                    valuesJson.getJSONArray(colName).toList().stream()
//                            .map(String::valueOf)
//                            .collect(Collectors.joining(","));
//        } else if ("BYTES".equals(colType)) {
//            colValue = "FROM_BASE64('" + valuesJson.getString(colName) + "')";
//        } else {
//            colValue = valuesJson.getString(colName);
//        }

        String response = "";

        // Ensure colValue is not null to avoid NullPointerExceptions
        if (colValue == null) {
            return response;
        }

        // Decode ByteBuffer into a string or appropriate type for processing
        String decodedValue = new String(colValue.array(), StandardCharsets.UTF_8);

        switch (columnType.toLowerCase()) {
            case "text":
            case "varchar":
            case "ascii":
                // Handle text-like types
                response = "'" + escapeCassandraString(decodedValue) + "'";
                break;

            case "timestamp":
            case "datetime":
                // Handle timestamp, adjust to UTC if necessary
                response = convertToCassandraTimestamp(decodedValue, sourceDbTimezoneOffset);
                break;

            case "uuid":
            case "timeuuid":
                // UUIDs should be properly formatted for Cassandra
                response = isValidUUID(decodedValue) ? decodedValue : "null";
                break;

            case "int":
            case "bigint":
            case "smallint":
            case "tinyint":
            case "varint":
            case "counter":
                // Handle integer-like types
                response = decodedValue;
                break;

            case "double":
            case "float":
            case "decimal":
                // Handle numeric types
                response = decodedValue;
                break;

            case "blob":
                // Handle binary data as hexadecimal for Cassandra
                response = "0x" + bytesToHex(colValue.array());
                break;

            case "boolean":
                // Cassandra uses true/false for boolean
                response = decodedValue.equalsIgnoreCase("true") || decodedValue.equals("1") ? "true" : "false";
                break;

            case "date":
                // Convert to Cassandra date format (YYYY-MM-DD)
                response = convertToCassandraDate(decodedValue);
                break;

            default:
                // For unsupported or unknown types, return the value as is
                response = decodedValue;
        }

        return response;
    }

    private Integer convertToCQLDate(com.google.cloud.Date spannerDate) {
        // Convert Google Cloud Date to an integer that represents the number of days since the epoch
        java.sql.Date sqlDate = java.sql.Date.valueOf(spannerDate.toString());
        long millis = sqlDate.getTime();
        return (int) (millis / (1000 * 60 * 60 * 24));
    }

    private static String escapeCassandraString(String value) {
        return value.replace("'", "''");
    }

    private static String convertToCassandraTimestamp(String value, String timezoneOffset) {
        // Parse the timestamp and adjust to UTC if needed
        ZonedDateTime dateTime = ZonedDateTime.parse(value);
        return "'" + dateTime.withZoneSameInstant(ZoneOffset.UTC).toString() + "'";
    }

    private static String convertToCassandraDate(String value) {
        // Convert to Cassandra's date format (YYYY-MM-DD)
        LocalDate date = LocalDate.parse(value);
        return "'" + date.toString() + "'";
    }

    private static boolean isValidUUID(String value) {
        try {
            UUID.fromString(value);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xFF & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
}
