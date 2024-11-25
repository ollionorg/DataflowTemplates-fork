package com.google.cloud.teleport.v2.templates.dml;

import com.google.cloud.teleport.v2.spanner.migrations.schema.*;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.google.cloud.spanner.Struct;
import com.google.gson.Gson;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.stream.Collectors;

/** Creates DML statements For Cassandra */
public class CassandraDMLGenerator implements IDMLGenerator{
    private static final Logger LOG = LoggerFactory.getLogger(CassandraDMLGenerator.class);
    /**
     * @param dmlGeneratorRequest the request containing necessary information to construct the DML
     *                            statement, including modification type, table schema, new values, and key values.
     * @return
     */
    @Override
    public DMLGeneratorResponse getDMLStatement(DMLGeneratorRequest dmlGeneratorRequest) {
        if (dmlGeneratorRequest
                .getSchema()
                .getSpannerToID()
                .get(dmlGeneratorRequest.getSpannerTableName())
                == null) {
            LOG.warn(
                    "The spanner table {} was not found in session file, dropping the record",
                    dmlGeneratorRequest.getSpannerTableName());
            return new DMLGeneratorResponse("");
        }

        String spannerTableId =
                dmlGeneratorRequest
                        .getSchema()
                        .getSpannerToID()
                        .get(dmlGeneratorRequest.getSpannerTableName())
                        .getName();
        SpannerTable spannerTable = dmlGeneratorRequest.getSchema().getSpSchema().get(spannerTableId);

        if (spannerTable == null) {
            LOG.warn(
                    "The spanner table {} was not found in session file, dropping the record",
                    dmlGeneratorRequest.getSpannerTableName());
            return new DMLGeneratorResponse("");
        }

        SourceTable sourceTable = dmlGeneratorRequest.getSchema().getSrcSchema().get(spannerTableId);
        if (sourceTable == null) {
            LOG.warn("The table {} was not found in source", dmlGeneratorRequest.getSpannerTableName());
            return new DMLGeneratorResponse("");
        }

        if (sourceTable.getPrimaryKeys() == null || sourceTable.getPrimaryKeys().length == 0) {
            LOG.warn(
                    "Cannot reverse replicate for table {} without primary key, skipping the record",
                    sourceTable.getName());
            return new DMLGeneratorResponse("");
        }

        Map<String, String> pkColumnNameValues =
                getPkColumnValues(
                        spannerTable,
                        sourceTable,
                        dmlGeneratorRequest.getNewValuesJson(),
                        dmlGeneratorRequest.getKeyValuesJson(),
                        dmlGeneratorRequest.getSourceDbTimezoneOffset());
        if (pkColumnNameValues == null) {
            LOG.warn(
                    "Cannot reverse replicate for table {} without primary key, skipping the record",
                    sourceTable.getName());
            return new DMLGeneratorResponse("");
        }
    }

    private static Map<String, String> getPkColumnValues(
            SpannerTable spannerTable,
            SourceTable sourceTable,
            JSONObject newValuesJson,
            JSONObject keyValuesJson,
            String sourceDbTimezoneOffset
    ) {
        Map<String, String> response = new HashMap<>();
        /*
        Get all primary key col ids from source table
        For each - get the corresponding column name from spanner Schema
        if the column cannot be found in spanner schema - return null
        check if the column name found in Spanner schema exists in keyJson -
          if so, get the string value
        else
        check if the column name found in Spanner schema exists in valuesJson -
          if so, get the string value
        if the column does not exist in any of the JSON - return null
        */
        ColumnPK[] sourcePKs = sourceTable.getPrimaryKeys();

        for (ColumnPK currentSourcePK : sourcePKs) {
            String colId = currentSourcePK.getColId();
            SourceColumnDefinition sourceColDef = sourceTable.getColDefs().get(colId);
            SpannerColumnDefinition spannerColDef = spannerTable.getColDefs().get(colId);
            if (spannerColDef == null) {
                LOG.warn(
                        "The corresponding primary key column {} was not found in Spanner",
                        sourceColDef.getName());
                return null;
            }
            String spannerColumnName = spannerColDef.getName();
            String columnValue = "";
            if (keyValuesJson.has(spannerColumnName)) {
                // get the value based on Spanner and Source type
                if (keyValuesJson.isNull(spannerColumnName)) {
                    response.put(sourceColDef.getName(), "NULL");
                    continue;
                }
                columnValue =
                        getMappedColumnValue(
                                spannerColDef,
                                sourceColDef,
                                keyValuesJson,
                                sourceDbTimezoneOffset
                        );
            } else if (newValuesJson.has(spannerColumnName)) {
                // get the value based on Spanner and Source type
                if (newValuesJson.isNull(spannerColumnName)) {
                    response.put(sourceColDef.getName(), "NULL");
                    continue;
                }
                columnValue =
                        getMappedColumnValue(
                                spannerColDef,
                                sourceColDef,
                                newValuesJson,
                                sourceDbTimezoneOffset
                        );
            } else {
                LOG.warn("The column {} was not found in input record", spannerColumnName);
                return null;
            }

            response.put(sourceColDef.getName(), columnValue);
        }

        return response;
    }

    private static String getMappedColumnValue(
            SpannerColumnDefinition spannerColDef,
            SourceColumnDefinition sourceColDef,
            JSONObject valuesJson,
            String sourceDbTimezoneOffset) {
        return TypeHandler.getColumnValueByType(
                spannerColDef ,
                sourceColDef,
                valuesJson,
                sourceDbTimezoneOffset
        );
    }

}



