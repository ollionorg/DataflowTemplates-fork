/*
 * Copyright (C) 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates.dbutils.dml;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnDefinition;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.InetAddress;
import java.net.UnknownHostException;
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

class CassandraTypeHandler {
    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link InetAddress} object containing InetAddress as value represented in cassandra type.
     */
    public static InetAddress handleCassandraInetAddressType(String colName, JSONObject valuesJson) throws UnknownHostException {
        return InetAddress.getByName(valuesJson.getString(colName));
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Boolean} object containing the value represented in cassandra type.
     */
    public static Boolean handleCassandraBoolType(String colName, JSONObject valuesJson) {
        return valuesJson.getBoolean(colName);
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Float} object containing the value represented in cassandra type.
     */
    public static Float handleCassandraFloatType(String colName, JSONObject valuesJson) {
        return valuesJson.getBigDecimal(colName).floatValue();
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Double} object containing the value represented in cassandra type.
     */
    public static Double handleCassandraDoubleType(String colName, JSONObject valuesJson) {
        return valuesJson.getBigDecimal(colName).doubleValue();
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link ByteBuffer} object containing the value represented in cassandra type.
     */
    public static ByteBuffer handleCassandraBlobType(String colName, JSONObject valuesJson) {
        Object colValue = valuesJson.opt(colName);
        if (colValue == null) {
            return null;
        }
        return parseBlobType(colName, colValue);

    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param colValue - contains all the key value for current incoming stream.
     *
     * @return a {@link ByteBuffer} object containing the value represented in cassandra type.
     */
    private static ByteBuffer parseBlobType(String colName, Object colValue) {
        byte[] byteArray;

        if (colValue instanceof byte[]) {
            byteArray = (byte[]) colValue;
        } else if (colValue instanceof String) {
            byteArray = java.util.Base64.getDecoder().decode((String) colValue);
        } else {
            throw new IllegalArgumentException("Unsupported type for column " + colName);
        }

        return ByteBuffer.wrap(byteArray);
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Date} object containing the value represented in cassandra type.
     */
    public static Date handleCassandraDateType(String colName, JSONObject valuesJson) {
        return handleCassandraGenericDateType(colName, valuesJson, "yyyy-MM-dd");
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Date} object containing timestamp as value represented in cassandra type.
     */
    public static Date handleCassandraTimestampType(String colName, JSONObject valuesJson) {
        return handleCassandraGenericDateType(colName, valuesJson, "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    }

    private static Date handleCassandraGenericDateType(String colName, JSONObject valuesJson, String formatter) {
        Object colValue = valuesJson.opt(colName);
        if (colValue == null) {
            return null;
        }

        if (formatter == null) {
            formatter = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
        }

        return parseDate(colName, colValue, formatter);
    }

    private static Date parseDate(String colName, Object colValue, String formatter) {
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

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link String} object containing String as value represented in cassandra type.
     */
    public static String handleCassandraTextType(String colName, JSONObject valuesJson) {
        return valuesJson.optString(colName, null); // Get the value or null if the key is not found or the value is null
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link UUID} object containing UUID as value represented in cassandra type.
     */
    public static UUID handleCassandraUuidType(String colName, JSONObject valuesJson) {
        String uuidString = valuesJson.optString(colName, null); // Get the value or null if the key is not found or the value is null

        if (uuidString == null) {
            return null;
        }

        return UUID.fromString(uuidString);

    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Long} object containing Long as value represented in cassandra type.
     */
    public static Long handleCassandraBigintType(String colName, JSONObject valuesJson) {
        return valuesJson.getBigInteger(colName).longValue();
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Integer} object containing Integer as value represented in cassandra type.
     */
    public static Integer handleCassandraIntType(String colName, JSONObject valuesJson) {
        return valuesJson.getBigInteger(colName).intValue();
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link List<Long>} object containing List<Long> as value represented in cassandra type.
     */
    public static List<Long> handleInt64ArrayType(String colName, JSONObject valuesJson) {
        JSONArray jsonArray = valuesJson.getJSONArray(colName);
        List<Long> colValueList = new ArrayList<>();

        // Convert each element to Long and add it to the list
        for (int i = 0; i < jsonArray.length(); i++) {
            colValueList.add(jsonArray.getLong(i));
        }

        return colValueList;
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Set<Long>} object containing Set<Long> as value represented in cassandra type.
     */
    public static Set<Long> handleInt64SetType(String colName, JSONObject valuesJson) {
        return new HashSet<>(handleInt64ArrayType(colName, valuesJson));
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link List<Integer>} object containing Set<Long> as value represented in cassandra type.
     */
    public static List<Integer> handleInt64ArrayAsInt32Array(String colName, JSONObject valuesJson) {
        return handleInt64ArrayType(colName, valuesJson).stream().map(Long::intValue).collect(Collectors.toList());
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Set<Integer>} object containing Set<Integer> as value represented in cassandra type.
     */
    public static Set<Integer> handleInt64ArrayAsInt32Set(String colName, JSONObject valuesJson) {
        return handleInt64ArrayType(colName, valuesJson).stream().map(Long::intValue).collect(Collectors.toSet());
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Set<String>} object containing Set<String> as value represented in cassandra type.
     */
    public static Set<String> handleStringSetType(String colName, JSONObject valuesJson) {
        return new HashSet<>(handleStringArrayType(colName, valuesJson));
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link List<String>} object containing List<String> as value represented in cassandra type.
     */
    public static List<String> handleStringArrayType(String colName, JSONObject valuesJson) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(String::valueOf)
                .collect(Collectors.toList());
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link List<Boolean>} object containing List<Boolean> as value represented in cassandra type.
     */
    public static List<Boolean> handleBoolArrayType(String colName, JSONObject valuesJson) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(obj -> obj instanceof String && Boolean.parseBoolean((String) obj))
                .collect(Collectors.toList());
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Set<Boolean>} object containing Set<Boolean> as value represented in cassandra type.
     */
    public static Set<Boolean> handleBoolSetTypeString(String colName, JSONObject valuesJson) {
        return new HashSet<>(handleBoolArrayType(colName, valuesJson));
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link List<Double>} object containing List<Double> as value represented in cassandra type.
     */
    public static List<Double> handleFloat64ArrayType(String colName, JSONObject valuesJson) {
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

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Set<Double>} object containing Set<Double> as value represented in cassandra type.
     */
    public static Set<Double> handleFloat64SetType(String colName, JSONObject valuesJson) {
        return new HashSet<>(handleFloat64ArrayType(colName, valuesJson));
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link List<Float>} object containing List<Float> as value represented in cassandra type.
     */
    public static List<Float> handleFloatArrayType(String colName, JSONObject valuesJson) {
        return handleFloat64ArrayType(colName, valuesJson).stream().map(Double::floatValue).collect(Collectors.toList());
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Set<Float>} object containing Set<Float> as value represented in cassandra type.
     */
    public static Set<Float> handleFloatSetType(String colName, JSONObject valuesJson) {
        return handleFloat64SetType(colName, valuesJson).stream().map(Double::floatValue).collect(Collectors.toSet());
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link List<Date>} object containing List<Date> as value represented in cassandra type.
     */
    public static List<Date> handleDateArrayType(String colName, JSONObject valuesJson) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(obj -> parseDate(colName, obj, "yyyy-MM-dd"))
                .collect(Collectors.toList());
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Set<Date>} object containing Set<Date> as value represented in cassandra type.
     */
    public static Set<Date> handleDateSetType(String colName, JSONObject valuesJson) {
        return new HashSet<>(handleDateArrayType(colName, valuesJson));
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link List<Timestamp>} object containing List<Timestamp> as value represented in cassandra type.
     */
    public static List<Timestamp> handleTimestampArrayType(String colName, JSONObject valuesJson) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(value -> {
                    return new Timestamp(parseDate(colName, value, "yyyy-MM-dd'T'HH:mm:ss.SSSX").getTime());
                })
                .collect(Collectors.toList());
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Set<Timestamp>} object containing Set<Timestamp> as value represented in cassandra type.
     */
    public static Set<Timestamp> handleTimestampSetType(String colName, JSONObject valuesJson) {
        return new HashSet<>(handleTimestampArrayType(colName, valuesJson));
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link List<ByteBuffer>} object containing List<ByteBuffer> as value represented in cassandra type.
     */
    public static List<ByteBuffer> handleByteArrayType(String colName, JSONObject valuesJson) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(value -> {
                    return parseBlobType(colName, value);
                })
                .collect(Collectors.toList());
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Set<ByteBuffer>} object containing Set<ByteBuffer> as value represented in cassandra type.
     */
    public static Set<ByteBuffer> handleByteSetType(String colName, JSONObject valuesJson) {
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

        switch (colType.toLowerCase()) {
            case "bigint":
            case "int64":
                colValue = CassandraTypeHandler.handleCassandraBigintType(colName, valuesJson);
                break;

            case "string":
                String inputValue = CassandraTypeHandler.handleCassandraTextType(colName, valuesJson);
                if(isValidUUID(inputValue)){
                    colValue = CassandraTypeHandler.handleCassandraUuidType(colName, valuesJson);
                } else if (isValidIPAddress(inputValue)) {
                    try {
                        colValue = CassandraTypeHandler.handleCassandraInetAddressType(colName, valuesJson);
                    } catch (UnknownHostException e) {
                        throw new RuntimeException(e);
                    }
                } else if(isValidJSON(inputValue)){
                    colValue= inputValue;
                } else {
                    colValue = "'" + escapeCassandraString(CassandraTypeHandler.handleCassandraTextType(colName, valuesJson)) + "'";
                }
                break;

            case "timestamp":
                colValue = convertToCassandraTimestamp(String.valueOf(
                        CassandraTypeHandler.handleCassandraTimestampType(colName, valuesJson)
                ), sourceDbTimezoneOffset);
                break;

            case "date":
            case "datetime":
                colValue = convertToCassandraTimestamp(String.valueOf(
                        CassandraTypeHandler.handleCassandraDateType(colName, valuesJson)
                ), sourceDbTimezoneOffset);
                break;

            case "boolean":
                colValue = CassandraTypeHandler.handleCassandraBoolType(colName, valuesJson);
                break;

            case "float64":
                colValue = CassandraTypeHandler.handleCassandraDoubleType(colName, valuesJson);
                break;

            case "numeric":
            case "float":
                colValue = CassandraTypeHandler.handleCassandraFloatType(colName, valuesJson);
                break;

            case "bytes(max)":
                colValue = CassandraTypeHandler.handleCassandraBlobType(colName, valuesJson);
                break;

            case "integer":
                colValue = CassandraTypeHandler.handleCassandraIntType(colName, valuesJson);
                break;
        }

        Object response = colValue;
        if(response == null){
            return response;
        }
        
        switch (columnType.toLowerCase()){

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

    private static boolean isValidIPAddress(String value){
        try {
            InetAddress.getByName(value);
            return true;
        } catch (UnknownHostException e) {
            return false;
        }
    }

    private static boolean isValidJSON(String value){
        try {
            new JSONObject(value);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
