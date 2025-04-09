/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.it.cassandra.CassandraResourceManager;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link SpannerToSourceDb} Flex template for all data types. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSourceDbWideRowBasicIT extends SpannerToSourceDbITBase {
  private static final String testName = "test_" + System.currentTimeMillis();

  @Test
  public void testAssert5000TablesPerDatabase() throws Exception {
    String databaseName = "rr-main-db-test-" + testName;
    SpannerResourceManager spannerResourceManagerForTables =
        SpannerResourceManager.builder(databaseName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    List<String> createTableQueries = getTablesCreatedDdlQueryStrings(5000);

    for (int i = 0; i < createTableQueries.size(); i += 100) {
      int end = Math.min(i + 100, createTableQueries.size());
      spannerResourceManagerForTables.executeDdlStatements(createTableQueries.subList(i, end));
    }

    String query =
        "SELECT COUNT(*) AS table_count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '' AND TABLE_SCHEMA = ''";
    ImmutableList<Struct> results = spannerResourceManagerForTables.runQuery(query);
    assertFalse(results.isEmpty());
    long tableCount = results.get(0).getLong(0);
    assertEquals(5000, tableCount);
    ResourceManagerUtils.cleanResources(spannerResourceManagerForTables);
  }

  private static @NotNull List<String> getTablesCreatedDdlQueryStrings(int size) {
    List<String> createTableQueries = new ArrayList<>();
    for (int tableNum = 1; tableNum <= size; tableNum++) {
      String tableName = "Table_" + tableNum;
      createTableQueries.add(
          String.format(
              "CREATE TABLE %s (\n"
                  + "  Id INT64 NOT NULL,\n"
                  + "  Name STRING(100)\n"
                  + ") PRIMARY KEY (Id)",
              tableName));
    }
    return createTableQueries;
  }

  @Test
  public void testCreateDatabaseAndTableWith1024Columns() throws Exception {
    String databaseName = "rr-main-table-per-columns-" + testName; // 🔹 Ensure unique DB name
    SpannerResourceManager spannerResourceManagerForColumnsPerTable =
        SpannerResourceManager.builder(databaseName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    StringBuilder createTableQuery = new StringBuilder("CREATE TABLE TestTable (\n");
    createTableQuery.append("    Id INT64 NOT NULL,\n");
    for (int i = 1; i < 1024; i++) {
      createTableQuery.append("    Col_").append(i).append(" STRING(100),\n");
    }
    createTableQuery.setLength(createTableQuery.length() - 2);
    createTableQuery.append("\n) PRIMARY KEY (Id)");

    spannerResourceManagerForColumnsPerTable.executeDdlStatement(createTableQuery.toString());

    String query =
        "SELECT COUNT(*) AS column_count FROM INFORMATION_SCHEMA.COLUMNS "
            + "WHERE TABLE_NAME = 'TestTable' AND TABLE_CATALOG = '' AND TABLE_SCHEMA = ''";

    ImmutableList<Struct> results = spannerResourceManagerForColumnsPerTable.runQuery(query);
    assertFalse(results.isEmpty());
    long columnCount = results.get(0).getLong(0);
    assertEquals(1024L, columnCount);
    ResourceManagerUtils.cleanResources(spannerResourceManagerForColumnsPerTable);
  }

  @Test
  public void testInsertValidCellSize_10MiB() {
    String databaseName = "rr-main-db-cell-size-valid-" + testName;
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder(databaseName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    String createTableQuery =
        "CREATE TABLE LargeDataTest ("
            + "  Id INT64 NOT NULL,"
            + "  LargeColumn STRING(MAX),"
            + ") PRIMARY KEY (Id)";
    spannerResourceManager.executeDdlStatement(createTableQuery);

    String validData = "A".repeat(2_621_440);

    spannerResourceManager.write(
        Mutation.newInsertBuilder("LargeDataTest")
            .set("Id")
            .to(1)
            .set("LargeColumn")
            .to(validData)
            .build());

    String query = "SELECT LENGTH(LargeColumn) FROM LargeDataTest WHERE Id = 1";
    ImmutableList<Struct> results = spannerResourceManager.runQuery(query);
    assertFalse(results.isEmpty());
    long columnSize = results.get(0).getLong(0);
    assertEquals(2_621_440, columnSize);
    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  @Test
  public void testInsertValidStringSize_2621440Characters() {
    String databaseName = "rr-main-db-string-size-valid-" + testName;
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder(databaseName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    String createTableQuery =
        "CREATE TABLE LargeStringTest ("
            + "  Id INT64 NOT NULL,"
            + "  LargeColumn STRING(2621440),"
            + ") PRIMARY KEY (Id)";
    spannerResourceManager.executeDdlStatement(createTableQuery);

    String validData = "A".repeat(2_621_440);

    spannerResourceManager.write(
        Mutation.newInsertBuilder("LargeStringTest")
            .set("Id")
            .to(1)
            .set("LargeColumn")
            .to(validData)
            .build());

    String query = "SELECT LENGTH(LargeColumn) FROM LargeStringTest WHERE Id = 1";
    ImmutableList<Struct> results = spannerResourceManager.runQuery(query);
    assertFalse(results.isEmpty());
    long columnSize = results.get(0).getLong(0);
    assertEquals(2_621_440, columnSize);

    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  @Test
  public void testInsertValidPrimaryKeySize_8KB() {
    String databaseName = "rr-main-db-key-valid-" + testName;
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder(databaseName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    String createTableQuery =
        "CREATE TABLE LargeKeyTest ("
            + "  LargeKey STRING(8192) NOT NULL,"
            + "  Value STRING(MAX)"
            + ") PRIMARY KEY (LargeKey)";
    spannerResourceManager.executeDdlStatement(createTableQuery);

    String validKey = "K".repeat(8192);

    spannerResourceManager.write(
        Mutation.newInsertBuilder("LargeKeyTest")
            .set("LargeKey")
            .to(validKey)
            .set("Value")
            .to("Some Data")
            .build());

    String query = "SELECT LENGTH(LargeKey) FROM LargeKeyTest";
    ImmutableList<Struct> results = spannerResourceManager.runQuery(query);
    assertFalse(results.isEmpty());
    long keySize = results.get(0).getLong(0);
    assertEquals(8192, keySize);

    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  @Test
  public void testTableKeyLengthNameWithSize48CharWithSpannerAndMysql() {
    String databaseName = "rr-main-db-key-valid-table-ms" + testName;
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder(databaseName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    MySQLResourceManager mySQLResourceManager = MySQLResourceManager.builder(testName).build();
    String tableName = "AB".repeat(32);
    String spannerCreateTableQuery =
        String.format(
            "CREATE TABLE %s ("
                + "  LargeKey STRING(8) NOT NULL,"
                + "  Value STRING(MAX)"
                + ") PRIMARY KEY (LargeKey)",
            tableName);
    spannerResourceManager.executeDdlStatement(spannerCreateTableQuery);
    String mysqlCreateTableQuery =
        String.format(
            "CREATE TABLE %s ("
                + "  LargeKey VARCHAR(8) PRIMARY KEY,"
                + "  Value VARCHAR(36)"
                + ")",
            tableName);

    mySQLResourceManager.runSQLUpdate(mysqlCreateTableQuery);
    assertEquals(0, mySQLResourceManager.getRowCount(tableName));
    ResourceManagerUtils.cleanResources(spannerResourceManager, mySQLResourceManager);
  }

  @Test
  public void testTableKeyLengthNameWithSize48CharWithSpannerAndCassandra() {
    String databaseName = "rr-main-db-key-valid-table-cs" + testName;
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder(databaseName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    CassandraResourceManager cassandraResourceManager = generateKeyspaceAndBuildCassandraResource();
    String tableName = "AB".repeat(24);
    String spannerCreateTableQuery =
        String.format(
            "CREATE TABLE %s ("
                + "  LargeKey STRING(8) NOT NULL,"
                + "  Value STRING(MAX)"
                + ") PRIMARY KEY (LargeKey)",
            tableName);
    spannerResourceManager.executeDdlStatement(spannerCreateTableQuery);
    String cassandraCreateTableQuery =
        String.format(
            "CREATE TABLE %s (" + "  LargeKey TEXT PRIMARY KEY," + "  Value TEXT" + ")", tableName);

    cassandraResourceManager.executeStatement(cassandraCreateTableQuery);
    assertEquals(0, getRowCount(cassandraResourceManager, tableName));
    ResourceManagerUtils.cleanResources(spannerResourceManager, cassandraResourceManager);
  }

  @Test
  public void testTableKeyLengthNameWithColumnNameSizeWithSpannerAndMysql() {
    String databaseName = "rr-main-db-key-valid-table-ms-mcx" + testName;
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder(databaseName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    MySQLResourceManager mySQLResourceManager = MySQLResourceManager.builder(testName).build();
    String tableName = "AB".repeat(32);
    String columnName = "AB".repeat(32);
    String spannerCreateTableQuery =
        String.format(
            "CREATE TABLE %s ("
                + "  LargeKey STRING(8) NOT NULL,"
                + "  %s STRING(MAX)"
                + ") PRIMARY KEY (LargeKey)",
            tableName, columnName);
    spannerResourceManager.executeDdlStatement(spannerCreateTableQuery);
    String mysqlCreateTableQuery =
        String.format(
            "CREATE TABLE %s (" + "  LargeKey VARCHAR(8) PRIMARY KEY," + "  %s VARCHAR(36)" + ")",
            tableName, columnName);

    mySQLResourceManager.runSQLUpdate(mysqlCreateTableQuery);
    assertEquals(0, mySQLResourceManager.getRowCount(tableName));
    ResourceManagerUtils.cleanResources(spannerResourceManager, mySQLResourceManager);
  }

  @Test
  public void testTableKeyLengthNameWithColumnNameSizeWithSpannerAndCassandra() {
    String databaseName = "rr-main-db-key-valid-table-cs-ccx" + testName;
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder(databaseName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    CassandraResourceManager cassandraResourceManager = generateKeyspaceAndBuildCassandraResource();
    String tableName = "AB".repeat(24);
    String columnName = "AB".repeat(16);
    String spannerCreateTableQuery =
        String.format(
            "CREATE TABLE %s ("
                + "  LargeKey STRING(36) NOT NULL,"
                + "  %s STRING(MAX)"
                + ") PRIMARY KEY (LargeKey)",
            tableName, columnName);
    spannerResourceManager.executeDdlStatement(spannerCreateTableQuery);
    String cassandraCreateTableQuery =
        String.format(
            "CREATE TABLE %s (" + "  LargeKey TEXT PRIMARY KEY," + "  %s TEXT" + ")",
            tableName, columnName);
    cassandraResourceManager.executeStatement(cassandraCreateTableQuery);
    assertEquals(0, getRowCount(cassandraResourceManager, tableName));
    ResourceManagerUtils.cleanResources(spannerResourceManager, cassandraResourceManager);
  }

  /**
   * Retrieves the total row count of a specified table in Cassandra.
   *
   * <p>This method executes a `SELECT COUNT(*)` query on the given table and returns the number of
   * rows present in it.
   *
   * @param tableName the name of the table whose row count is to be retrieved.
   * @return the total number of rows in the specified table.
   * @throws RuntimeException if the query does not return a result.
   */
  private long getRowCount(CassandraResourceManager cassandraResourceManager, String tableName) {
    String query = String.format("SELECT COUNT(*) FROM %s", tableName);
    ResultSet resultSet = cassandraResourceManager.executeStatement(query);
    Row row = resultSet.one();
    if (row != null) {
      return row.getLong(0);
    } else {
      throw new RuntimeException("Query did not return a result for table: " + tableName);
    }
  }
}
