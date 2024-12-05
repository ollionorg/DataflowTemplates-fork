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
import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

class TypeHandler {
    public static Boolean handleCassandraBoolType(String colName, JSONObject valuesJson) {
        return valuesJson.getBoolean(colName);
    }

    public static Float handleCassandraFloatType(String colName, JSONObject valuesJson) {
        return valuesJson.getBigDecimal(colName).floatValue();
    }

    public static Double handleCassandraDoubleType(String colName, JSONObject valuesJson) {
        return valuesJson.getBigDecimal(colName).doubleValue();
    }

    public static ByteBuffer handleCassandraBlobType(String colName, JSONObject valuesJson) {
        Object colValue = valuesJson.opt(colName);
        if (colValue == null) {
            return null;
        }
        return parseBlobType(colName, colValue);

    }

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

    public static Date handleCassandraDateType(String colName, JSONObject valuesJson) {
        return handleCassandraGenericDateType(colName, valuesJson, "yyyy-MM-dd");
    }

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

    public static String handleCassandraTextType(String colName, JSONObject valuesJson) {
        return valuesJson.optString(colName, null); // Get the value or null if the key is not found or the value is null
    }

    public static UUID handleCassandraUuidType(String colName, JSONObject valuesJson) {
        String uuidString = valuesJson.optString(colName, null); // Get the value or null if the key is not found or the value is null

        if (uuidString == null) {
            return null;
        }

        return UUID.fromString(uuidString);

    }

    public static Long handleCassandraBigintType(String colName, JSONObject valuesJson) {
        return valuesJson.getBigInteger(colName).longValue();
    }

    public static Integer handleCassandraIntType(String colName, JSONObject valuesJson) {
        return valuesJson.getBigInteger(colName).intValue();
    }

    public static List<Long> handleInt64ArrayType(String colName, JSONObject valuesJson) {
        JSONArray jsonArray = valuesJson.getJSONArray(colName);
        List<Long> colValueList = new ArrayList<>();

        // Convert each element to Long and add it to the list
        for (int i = 0; i < jsonArray.length(); i++) {
            colValueList.add(jsonArray.getLong(i));
        }

        return colValueList;
    }

    public static Set<Long> handleInt64SetType(String colName, JSONObject valuesJson) {
        return new HashSet<>(handleInt64ArrayType(colName, valuesJson));
    }

    public static List<Integer> handleInt64ArrayAsInt32Array(String colName, JSONObject valuesJson) {
        return handleInt64ArrayType(colName, valuesJson).stream().map(Long::intValue).collect(Collectors.toList());
    }

    public static Set<Integer> handleInt64ArrayAsInt32Set(String colName, JSONObject valuesJson) {
        return handleInt64ArrayType(colName, valuesJson).stream().map(Long::intValue).collect(Collectors.toSet());
    }

    public static Set<String> handleStringArrayType(String colName, JSONObject valuesJson) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(String::valueOf)
                .collect(Collectors.toSet());
    }

    public static List<String> handleStringSetType(String colName, JSONObject valuesJson) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(String::valueOf)
                .collect(Collectors.toList());
    }

    // Handler for ARRAY<Boolean> (also serves as Set<Boolean>)
    public static List<Boolean> handleBoolArrayType(String colName, JSONObject valuesJson) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(obj -> obj instanceof String && Boolean.parseBoolean((String) obj))
                .collect(Collectors.toList());
    }

    public static Set<Boolean> handleBoolSetTypeString(String colName, JSONObject valuesJson) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(obj -> obj instanceof String && Boolean.parseBoolean((String) obj))
                .collect(Collectors.toSet());
    }

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

    public static Set<Double> handleFloat64SetType(String colName, JSONObject valuesJson) {
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

    public static List<Float> handleFloatArrayType(String colName, JSONObject valuesJson) {
        return handleFloat64ArrayType(colName, valuesJson).stream().map(Double::floatValue).collect(Collectors.toList());
    }

    public static Set<Float> handleFloatSetType(String colName, JSONObject valuesJson) {
        return handleFloat64SetType(colName, valuesJson).stream().map(Double::floatValue).collect(Collectors.toSet());
    }

    public static List<Date> handleDateArrayType(String colName, JSONObject valuesJson) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(obj -> parseDate(colName, obj, "yyyy-MM-dd"))
                .collect(Collectors.toList());
    }

    public static Set<Date> handleDateSetType(String colName, JSONObject valuesJson) {
        return new HashSet<>(handleDateArrayType(colName, valuesJson));
    }

    public static List<Timestamp> handleTimestampArrayType(String colName, JSONObject valuesJson) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(value -> {
                    return new Timestamp(parseDate(colName, value, "yyyy-MM-dd'T'HH:mm:ss.SSSX").getTime());
                })
                .collect(Collectors.toList());
    }

    public static Set<Timestamp> handleTimestampSetType(String colName, JSONObject valuesJson) {
        return new HashSet<>(handleTimestampArrayType(colName, valuesJson));
    }

    public static List<ByteBuffer> handleByteArrayType(String colName, JSONObject valuesJson) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(value -> {
                    return parseBlobType(colName, value);
                })
                .collect(Collectors.toList());
    }

    public static Set<ByteBuffer> handleByteSetType(String colName, JSONObject valuesJson) {
        return new HashSet<>(handleByteArrayType(colName, valuesJson));
    }
}
