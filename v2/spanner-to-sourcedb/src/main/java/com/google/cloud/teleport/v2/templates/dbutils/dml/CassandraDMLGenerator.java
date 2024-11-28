package com.google.cloud.teleport.v2.templates.dml;

import com.google.cloud.teleport.v2.spanner.migrations.schema.*;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Creates DML statements For Cassandra
 */
public class CassandraDMLGenerator implements IDMLGenerator {
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

        Map<String, Object> pkColumnNameValues =
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

        if ("INSERT".equals(dmlGeneratorRequest.getModType())
                || "UPDATE".equals(dmlGeneratorRequest.getModType())) {
            return generateUpsertStatement(
                    spannerTable,
                    sourceTable,
                    dmlGeneratorRequest,
                    pkColumnNameValues
            );

        } else if ("DELETE".equals(dmlGeneratorRequest.getModType())) {
            return new DMLGeneratorResponse(
                    getDeleteStatementCQL(sourceTable.getName(), pkColumnNameValues)
            );
        } else {
            LOG.warn("Unsupported modType: " + dmlGeneratorRequest.getModType());
            return new DMLGeneratorResponse("");
        }
    }

    private static DMLGeneratorResponse generateUpsertStatement(
            SpannerTable spannerTable,
            SourceTable sourceTable,
            DMLGeneratorRequest dmlGeneratorRequest,
            Map<String, Object> pkColumnNameValues) {
        Map<String, Object> columnNameValues =
                getColumnValues(
                        spannerTable,
                        sourceTable,
                        dmlGeneratorRequest.getNewValuesJson(),
                        dmlGeneratorRequest.getKeyValuesJson(),
                        dmlGeneratorRequest.getSourceDbTimezoneOffset());
        return new DMLGeneratorResponse(
                getUpsertStatementCQL(
                        sourceTable.getName(),
                        sourceTable.getPrimaryKeySet(),
                        columnNameValues,
                        pkColumnNameValues
                )
        );
    }

    private static String getUpsertStatementCQL(
            String tableName,
            Set<String> primaryKeys,
            Map<String, Object> columnNameValues,
            Map<String, Object> pkColumnNameValues) {

        StringBuilder allColumns = new StringBuilder();
        StringBuilder allValues = new StringBuilder();

        // Process primary key columns
        for (Map.Entry<String, Object> entry : pkColumnNameValues.entrySet()) {
            String colName = entry.getKey();
            Object colValue = entry.getValue();

            allColumns.append(colName).append(", ");
            allValues.append(colValue).append(", ");
        }

        // Process additional columns
        for (Map.Entry<String, Object> entry : columnNameValues.entrySet()) {
            String colName = entry.getKey();
            Object colValue = entry.getValue();

            allColumns.append(colName).append(", ");
            allValues.append(colValue).append(", ");
        }

        // Remove trailing comma and space
        if (allColumns.length() > 0) {
            allColumns.setLength(allColumns.length() - 2);
        }
        if (allValues.length() > 0) {
            allValues.setLength(allValues.length() - 2);
        }

        // Construct the CQL INSERT statement
        return "INSERT INTO " + tableName + " (" + allColumns + ") VALUES (" + allValues + ");";
    }

    private static String getDeleteStatementCQL(
            String tableName, Map<String, Object> pkColumnNameValues) {

        StringBuilder deleteConditions = new StringBuilder();

        // Process primary key columns for the WHERE clause
        int index = 0;
        for (Map.Entry<String, Object> entry : pkColumnNameValues.entrySet()) {
            String colName = entry.getKey();
            Object colValue = entry.getValue();

            deleteConditions.append(colName).append(" = ").append(colValue);
            if (index + 1 < pkColumnNameValues.size()) {
                deleteConditions.append(" AND ");
            }
            index++;
        }

        // Construct the CQL DELETE statement
        return "DELETE FROM " + tableName + " WHERE " + deleteConditions + ";";
    }

    private static Map<String, Object> getColumnValues(
            SpannerTable spannerTable,
            SourceTable sourceTable,
            JSONObject newValuesJson,
            JSONObject keyValuesJson,
            String sourceDbTimezoneOffset
    ) {
        Map<String, Object> response = new HashMap<>();

    /*
    Get all non-primary key col ids from source table
    For each - get the corresponding column name from spanner Schema
    if the column cannot be found in spanner schema - continue to next,
      as the column will be stored with default/null values
    check if the column name found in Spanner schema exists in keyJson -
      if so, get the string value
    else
    check if the column name found in Spanner schema exists in valuesJson -
      if so, get the string value
    if the column does not exist in any of the JSON - continue to next,
      as the column will be stored with default/null values
    */
        Set<String> sourcePKs = sourceTable.getPrimaryKeySet();
        for (Map.Entry<String, SourceColumnDefinition> entry : sourceTable.getColDefs().entrySet()) {
            SourceColumnDefinition sourceColDef = entry.getValue();

            String colName = sourceColDef.getName();
            if (sourcePKs.contains(colName)) {
                continue; // we only need non-primary keys
            }

            String colId = entry.getKey();
            SpannerColumnDefinition spannerColDef = spannerTable.getColDefs().get(colId);
            if (spannerColDef == null) {
                continue;
            }
            String spannerColumnName = spannerColDef.getName();
            Object columnValue;
            if (keyValuesJson.has(spannerColumnName)) {
                // get the value based on Spanner and Source type
                if (keyValuesJson.isNull(spannerColumnName)) {
                    response.put(sourceColDef.getName(), "NULL");
                    continue;
                }
                columnValue =
                        getMappedColumnValue(
                                spannerColDef, sourceColDef, keyValuesJson, sourceDbTimezoneOffset);
            } else if (newValuesJson.has(spannerColumnName)) {
                // get the value based on Spanner and Source type
                if (newValuesJson.isNull(spannerColumnName)) {
                    response.put(sourceColDef.getName(), "NULL");
                    continue;
                }
                columnValue =
                        getMappedColumnValue(
                                spannerColDef, sourceColDef, newValuesJson, sourceDbTimezoneOffset);
            } else {
                continue;
            }

            response.put(sourceColDef.getName(), columnValue);
        }

        return response;
    }

    private static Map<String, Object> getPkColumnValues(
            SpannerTable spannerTable,
            SourceTable sourceTable,
            JSONObject newValuesJson,
            JSONObject keyValuesJson,
            String sourceDbTimezoneOffset
    ) {
        Map<String, Object> response = new HashMap<>();
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
            Object columnValue;
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

    private static Object getMappedColumnValue(
            SpannerColumnDefinition spannerColDef,
            SourceColumnDefinition sourceColDef,
            JSONObject valuesJson,
            String sourceDbTimezoneOffset) {
        return TypeHandler.getColumnValueByType(
                spannerColDef,
                sourceColDef,
                valuesJson,
                sourceDbTimezoneOffset
        );
    }

}



