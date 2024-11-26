package com.google.cloud.teleport.v2.templates.dml;

import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnDefinition;
import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

class TypeHandler {
    private static Boolean handleCassandraBoolType(String colName, JSONObject valuesJson) {
        return valuesJson.getBoolean(colName);
    }

    private static Float handleCassandraFloatType(String colName, JSONObject valuesJson) {
        return valuesJson.getBigDecimal(colName).floatValue();
    }

    private static Double handleCassandraDoubleType(String colName, JSONObject valuesJson) {
        return valuesJson.getBigDecimal(colName).doubleValue();
    }

    private static ByteBuffer handleCassandraBlobType(String colName, JSONObject valuesJson) {
        Object colValue = valuesJson.opt(colName);
        if (colValue == null) {
            return null;
        }
        return parseBlobType(colName, colValue);

    }

    private static ByteBuffer parseBlobType(String colName, Object colValue){
        byte[] byteArray;

        if (colValue instanceof byte[]) {
            byteArray = (byte[]) colValue;
        } else if (colValue instanceof String) {
            byteArray = java.util.Base64.getDecoder().decode((String) colValue);
        }
        else {
            throw new IllegalArgumentException("Unsupported type for column " + colName);
        }

        return ByteBuffer.wrap(byteArray);
    }

    private static Date handleCassandraDateType(String colName, JSONObject valuesJson) {
        return handleCassandraGenericDateType(colName, valuesJson, "yyyy-MM-dd");
    }

    private static Date handleCassandraTimestampType(String colName, JSONObject valuesJson) {
        return handleCassandraGenericDateType(colName, valuesJson, "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    }

    private static Date handleCassandraGenericDateType(String colName, JSONObject valuesJson, String formatter) {
        Object colValue = valuesJson.opt(colName);
        if (colValue == null) {
            return null;
        }

        if(formatter == null){
            formatter = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
        }

        return parseDate(colName, colValue, formatter);
    }

    private static  Date parseDate(String colName, Object colValue, String formatter){
        Date date;

        if (colValue instanceof String) {
            try {
                date = new SimpleDateFormat(formatter).parse((String) colValue);
            } catch (ParseException e) {
                throw new IllegalArgumentException("Invalid timestamp format for column " + colName, e);
            }
        } else if (colValue instanceof java.util.Date) {
            date = (java.util.Date) colValue;
        } else if (colValue instanceof Long) {
            date = new Date((Long) colValue);
        } else {
            throw new IllegalArgumentException("Unsupported type for column " + colName);
        }

        return date;
    }

    private static String handleCassandraTextType(String colName, JSONObject valuesJson) {
        return valuesJson.optString(colName, null); // Get the value or null if the key is not found or the value is null
    }

    private static UUID handleCassandraUuidType(String colName, JSONObject valuesJson) {
        String uuidString = valuesJson.optString(colName, null); // Get the value or null if the key is not found or the value is null

        if (uuidString == null) {
            return null;
        }

        return UUID.fromString(uuidString);

    }

    private static Long handleCassandraBigintType(String colName, JSONObject valuesJson) {
        return valuesJson.getBigInteger(colName).longValue();
    }

    private static Integer handleCassandraIntType(String colName, JSONObject valuesJson) {
        return valuesJson.getBigInteger(colName).intValue();
    }

    private static List<Long> handleInt64ArrayType(String colName, JSONObject valuesJson) {
        JSONArray jsonArray = valuesJson.getJSONArray(colName);
        List<Long> colValueList = new ArrayList<>();

        // Convert each element to Long and add it to the list
        for (int i = 0; i < jsonArray.length(); i++) {
            colValueList.add(jsonArray.getLong(i));
        }

        return colValueList;
    }

    private static Set<Long> handleInt64SetType(String colName, JSONObject valuesJson) {
        return new HashSet<>(handleInt64ArrayType(colName, valuesJson));
    }

    private static List<Integer> handleInt64ArrayAsInt32Array(String colName, JSONObject valuesJson) {
        return handleInt64ArrayType(colName, valuesJson).stream().map(Long::intValue).collect(Collectors.toList());
    }

    private static Set<Integer> handleInt64ArrayAsInt32Set(String colName, JSONObject valuesJson) {
        return handleInt64ArrayType(colName, valuesJson).stream().map(Long::intValue).collect(Collectors.toSet());
    }

    private static Set<String> handleStringArrayType(String colName, JSONObject valuesJson) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(String::valueOf)
                .collect(Collectors.toSet());
    }

    private static List<String> handleStringSetType(String colName, JSONObject valuesJson) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(String::valueOf)
                .collect(Collectors.toList());
    }

    // Handler for ARRAY<Boolean> (also serves as Set<Boolean>)
    private static List<Boolean> handleBoolArrayType(String colName, JSONObject valuesJson) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(obj -> obj instanceof String && Boolean.parseBoolean((String) obj))
                .collect(Collectors.toList());
    }

    private static Set<Boolean> handleBoolSetTypeString(String colName, JSONObject valuesJson) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(obj -> obj instanceof String && Boolean.parseBoolean((String) obj))
                .collect(Collectors.toSet());
    }

    private static List<Double> handleFloat64ArrayType(String colName, JSONObject valuesJson) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(obj -> {
                    if (obj instanceof Number) {
                        return ((Number) obj).doubleValue();
                    } else if (obj instanceof String) {
                        try {
                            return Double.valueOf((String) obj);
                        } catch (NumberFormatException e) {
                            throw new IllegalArgumentException("Invalid number format for column " + colName, e);
                        }
                    } else {
                        throw new IllegalArgumentException("Unsupported type for column " + colName);
                    }
                })
                .collect(Collectors.toList());
    }

    private static Set<Double> handleFloat64SetType(String colName, JSONObject valuesJson) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(obj -> {
                    if (obj instanceof Number) {
                        return ((Number) obj).doubleValue();
                    } else if (obj instanceof String) {
                        try {
                            return Double.valueOf((String) obj);
                        } catch (NumberFormatException e) {
                            throw new IllegalArgumentException("Invalid number format for column " + colName, e);
                        }
                    } else {
                        throw new IllegalArgumentException("Unsupported type for column " + colName);
                    }
                })
                .collect(Collectors.toSet());
    }

    private static List<Float> handleFloatArrayType(String colName, JSONObject valuesJson) {
        return handleFloat64ArrayType(colName, valuesJson).stream().map(Double::floatValue).collect(Collectors.toList());
    }

    private static Set<Float> handleFloatSetType(String colName, JSONObject valuesJson) {
        return handleFloat64SetType(colName, valuesJson).stream().map(Double::floatValue).collect(Collectors.toSet());
    }

    // Handler for ARRAY<Date> (also serves as Set<Date>)
    private static List<Date> handleDateArrayType(String colName, JSONObject valuesJson) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(obj -> parseDate(colName, obj, "yyyy-MM-dd"))
                .collect(Collectors.toList());
    }

    private static Set<Date> handleDateSetType(String colName, JSONObject valuesJson) {
        return new HashSet<>(handleDateArrayType(colName, valuesJson));
    }

    // Handler for ARRAY<Timestamp> (also serves as Set<Timestamp>)
    private static List<Timestamp> handleTimestampArrayType(String colName, JSONObject valuesJson) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(value -> {
                    return Timestamp.valueOf(parseDate(colName, value, "yyyy-MM-dd'T'HH:mm:ss.SSSZ").toString());
                })
                .collect(Collectors.toList());
    }

    private static Set<Timestamp> handleTimestampSetType(String colName, JSONObject valuesJson) {
        return new HashSet<>(handleTimestampArrayType(colName, valuesJson));
    }

    private static List<ByteBuffer> handleByteArrayType(String colName, JSONObject valuesJson) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(value -> {
                    return parseBlobType(colName, value);
                })
                .collect(Collectors.toList());
    }

    private static Set<ByteBuffer> handleByteSetType(String colName, JSONObject valuesJson) {
        return new HashSet<>(handleByteArrayType(colName, valuesJson));
    }

    public static Object getColumnValueByType(
            SpannerColumnDefinition spannerColDef,
            SourceColumnDefinition sourceColDef,
            JSONObject valuesJson,
            String sourceDbTimezoneOffset
    ) {

        String columnType = sourceColDef.getType().getName();
        Object colValue = null;
        String colType = spannerColDef.getType().getName();
        String colName = spannerColDef.getName();

        if ("FLOAT64".equals(colType)) {
            colValue = TypeHandler.handleCassandraFloatType(colName, valuesJson);
        } else if ("BOOL".equals(colType)) {
            colValue = TypeHandler.handleCassandraBoolType(colName, valuesJson);
        } else if ("STRING".equals(colType) && spannerColDef.getType().getIsArray()) {
            colValue = TypeHandler.handleStringArrayType(colName, valuesJson);
        } else if ("BYTES".equals(colType)) {
            colValue = TypeHandler.handleCassandraBlobType(colName, valuesJson);
        } else {
            colValue = TypeHandler.handleCassandraTextType(colName, valuesJson);
        }

        Object response = null;
        // Ensure colValue is not null to avoid NullPointerExceptions
        if (colValue == null) {
            return colValue;
        }

        switch (columnType.toLowerCase()) {
            case "text":
            case "varchar":
            case "ascii":
                // Handle text-like types
                response = "'" + escapeCassandraString(String.valueOf(colValue)) + "'";
                break;

            case "timestamp":
            case "datetime":
                // Handle timestamp, adjust to UTC if necessary
                response = convertToCassandraTimestamp(String.valueOf(colValue), sourceDbTimezoneOffset);
                break;

            case "uuid":
            case "timeuuid":
                // UUIDs should be properly formatted for Cassandra
                response = isValidUUID(String.valueOf(colValue)) ? colValue : "null";
                break;

            case "int":
            case "bigint":
            case "smallint":
            case "tinyint":
            case "varint":
            case "counter":
            case "double":
            case "float":
            case "decimal":
            case "blob":
            case "boolean":
               response = colValue;
                break;
            case "date":
                response = convertToCassandraDate((Date) colValue);
                break;

            default:
                throw new IllegalArgumentException("Invalid column " + colName + " do not have mapping created for "+columnType);
        }

        return response;
    }

    private static Integer convertToCQLDate(com.google.cloud.Date spannerDate) {
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

    private static String convertToCassandraDate(Date date) {
        LocalDate localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        return localDate.toString();  // Returns the date in the format "yyyy-MM-dd"
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
