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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.MultipleFailureException;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToCassandraSourceDbDatatypeIT extends SpannerToCassandraDbITBase {

  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToCassandraSourceDbDatatypeIT/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "SpannerToCassandraSourceDbDatatypeIT/session.json";
  private static final String CASSANDRA_SCHEMA_FILE_RESOURCE =
      "SpannerToCassandraSourceDbDatatypeIT/cassandra-schema.sql";

  private static final String TABLE = "AllDatatypeColumns";
  private static final HashSet<SpannerToCassandraSourceDbDatatypeIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  public static CassandraSharedResourceManager cassandraResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;
  private final List<Throwable> assertionErrors = new ArrayList<>();

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (SpannerToCassandraSourceDbDatatypeIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        cassandraResourceManager = generateKeyspaceAndBuildCassandraResource();
        gcsResourceManager =
            GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
                .build();
        createCassandraSchema(cassandraResourceManager, CASSANDRA_SCHEMA_FILE_RESOURCE);
        createAndUploadCassandraConfigToGcs(gcsResourceManager, cassandraResourceManager);
        gcsResourceManager.uploadArtifact(
            "input/session.json", Resources.getResource(SESSION_FILE_RESOURCE).getPath());
        pubsubResourceManager = setUpPubSubResourceManager();
        subscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager).replace("gs://" + artifactBucketName, ""));
        jobInfo =
            launchDataflowJob(
                gcsResourceManager,
                spannerResourceManager,
                spannerMetadataResourceManager,
                subscriptionName.toString(),
                null,
                null,
                null,
                null,
                null);
      }
    }
  }

  /**
   * Cleanup dataflow job and all the resources and resource managers.
   *
   * @throws IOException
   */
  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerToCassandraSourceDbDatatypeIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        cassandraResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void spannerToCassandraSourceDataTypeConversionTest()
      throws InterruptedException, IOException, MultipleFailureException {
    assertThatPipeline(jobInfo).isRunning();
    writeRowInSpanner();
    assertRowInCassandraDB();
  }

  private long getRowCount() {
    String query = String.format("SELECT COUNT(*) FROM %s", TABLE);
    ResultSet resultSet = cassandraResourceManager.executeStatement(query);
    Row row = resultSet.one();
    if (row != null) {
      return row.getLong(0);
    } else {
      throw new RuntimeException("Query did not return a result for table: " + TABLE);
    }
  }

  private void writeRowInSpanner() {
    Mutation mutation =
        Mutation.newInsertOrUpdateBuilder(TABLE)
            .set("varchar_column")
            .to("value1")
            .set("tinyint_column")
            .to(10)
            .set("text_column")
            .to("text_column_value")
            .set("date_column")
            .to(Value.date(Date.fromJavaUtilDate(java.sql.Date.valueOf("2024-05-24"))))
            .set("smallint_column")
            .to(50)
            .set("mediumint_column")
            .to(1000)
            .set("int_column")
            .to(50000)
            .set("bigint_column")
            .to(987654321L)
            .set("float_column")
            .to(45.67)
            .set("double_column")
            .to(123.789)
            .set("decimal_column")
            .to(new BigDecimal("1234.56"))
            .set("datetime_column")
            .to(Value.timestamp(Timestamp.parseTimestamp("2024-02-08T08:15:30Z")))
            .set("timestamp_column")
            .to(Value.timestamp(Timestamp.parseTimestamp("2024-02-08T08:15:30Z")))
            .set("time_column")
            .to("14:30:00")
            .set("year_column")
            .to("2022")
            .set("char_column")
            .to("char_col")
            .set("tinytext_column")
            .to("tinytext_column_value")
            .set("mediumtext_column")
            .to("mediumtext_column_value")
            .set("longtext_column")
            .to("longtext_column_value")
            .set("enum_column")
            .to("2")
            .set("bool_column")
            .to(Value.bool(Boolean.FALSE))
            .set("other_bool_column")
            .to(Value.bool(Boolean.TRUE))
            .set("bytes_column")
            .to(Value.bytes(ByteArray.copyFrom(new byte[] {0x01, 0x02, 0x03, 0x04})))
            .set("varint_column")
            .to(
                Value.bytes(
                    ByteArray.copyFrom("12345678901234567890".getBytes()))) // Varint as bytes
            // List Columns
            .set("list_text_column")
            .to(Value.json("[\"apple\", \"banana\", \"cherry\"]")) // List of text
            .set("list_int_column")
            .to(Value.json("[1, 2, 3, 4]")) // List of integers
            .set("frozen_list_bigint_column")
            .to(
                Value.json(
                    "[10000000000, 20000000000, 30000000000]")) // Frozen list of big integers

            // Set Columns
            .set("set_text_column")
            .to(Value.json("[\"carrot\", \"lettuce\", \"spinach\"]")) // Set of text
            .set("set_date_column")
            .to(Value.json("[\"2024-05-01\", \"2024-06-01\"]")) // Set of dates
            .set("frozen_set_bool_column")
            .to(Value.json("[true, false]")) // Frozen set of booleans

            // Map Columns
            .set("map_text_to_int_column")
            .to(Value.json("{\"key1\": 1, \"key2\": 2, \"key3\": 3}")) // Map of text to int
            .set("map_date_to_text_column")
            .to(
                Value.json(
                    "{\"2024-05-01\": \"Value1\", \"2024-06-01\": \"Value2\"}")) // Map of date to
            // text
            .set("frozen_map_int_to_bool_column")
            .to(Value.json("{\"1\": true, \"2\": false}")) // Frozen map of int to bool

            // Combinations of Collections
            .set("map_text_to_list_column")
            .to(
                Value.json(
                    "{\"key1\": [\"item1\", \"item2\"], \"key2\": [\"item3\", \"item4\"]}")) // Map
            .set("map_text_to_set_column")
            .to(
                Value.json(
                    "{\"key1\": [1, 2], \"key2\": [3, 4]}")) // Map of text to set of integers
            .set("set_of_maps_column")
            .to(Value.json("[{\"key1\": 1}, {\"key2\": 2}]")) // Set of maps (text -> int)
            .set("list_of_sets_column")
            .to(
                Value.json(
                    "[[\"apple\", \"banana\"], [\"cherry\", \"date\"]]")) // List of sets of text

            // Frozen Combinations
            .set("frozen_map_text_to_list_column")
            .to(
                Value.json(
                    "{\"key1\": [\"item1\", \"item2\"]}")) // Frozen map of text to list of text
            .set("frozen_map_text_to_set_column")
            .to(
                Value.json(
                    "{\"key1\": [\"item1\", \"item2\"]}")) // Frozen map of text to set of text
            .set("frozen_set_of_maps_column")
            .to(Value.json("[{\"key1\": 1}, {\"key2\": 2}]")) // Frozen set of maps (text -> int)
            .set("frozen_list_of_sets_column")
            .to(
                Value.json(
                    "[[\"item1\", \"item2\"], [\"item3\", \"item4\"]]")) // Frozen list of sets of
            // text
            .build();

    spannerResourceManager.write(mutation);
  }

  private void assertAll(Runnable... assertions) throws MultipleFailureException {
    for (Runnable assertion : assertions) {
      try {
        assertion.run();
      } catch (AssertionError e) {
        assertionErrors.add(e);
      }
    }
    if (!assertionErrors.isEmpty()) {
      throw new MultipleFailureException(assertionErrors);
    }
  }

  private void assertRowInCassandraDB() throws InterruptedException, MultipleFailureException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)), () -> getRowCount() == 1);
    assertThatResult(result).meetsConditions();
    Iterable<Row> rows;
    try {
      rows = cassandraResourceManager.readTable(TABLE);
    } catch (Exception e) {
      throw new RuntimeException("Failed to read from Cassandra table: " + TABLE, e);
    }

    assertThat(rows).hasSize(1);

    Row row = rows.iterator().next();
    System.out.println(row.getFormattedContents());

    assertThat(rows).hasSize(1);
    assertAll(
        () -> assertThat(row.getString("varchar_column")).isEqualTo("value1"),
        () -> assertThat(row.getInt("tinyint_column")).isEqualTo(10),
        () -> assertThat(row.getString("text_column")).isEqualTo("text_column_value"),
        () ->
            assertThat(row.getLocalDate("date_column"))
                .isEqualTo(java.time.LocalDate.of(2024, 5, 24)),
        () -> assertThat(row.getShort("smallint_column")).isEqualTo((short) 50),
        () -> assertThat(row.getInt("mediumint_column")).isEqualTo(1000),
        () -> assertThat(row.getInt("int_column")).isEqualTo(50000),
        () -> assertThat(row.getLong("bigint_column")).isEqualTo(987654321L),
        () -> {
          float expectedFloat = 45.67f;
          float actualFloat = row.getFloat("float_column");
          assertThat(Math.abs(actualFloat - expectedFloat)).isLessThan(0.001f);
        },
        () -> {
          double expectedDouble = 123.789;
          double actualDouble = row.getDouble("double_column");
          assertThat(Math.abs(actualDouble - expectedDouble)).isLessThan(0.001);
        },
        () -> assertThat(row.getBigDecimal("decimal_column")).isEqualTo(new BigDecimal("1234.56")),
        () ->
            assertThat(row.getInstant("datetime_column"))
                .isEqualTo(java.time.Instant.parse("2024-02-08T08:15:30Z")),
        () ->
            assertThat(row.getInstant("timestamp_column"))
                .isEqualTo(java.time.Instant.parse("2024-02-08T08:15:30Z")),
        () ->
            assertThat(row.getLocalTime("time_column"))
                .isEqualTo(java.time.LocalTime.of(14, 30, 0)),
        () -> assertThat(row.getString("year_column")).isEqualTo("2022"),
        () -> assertThat(row.getString("char_column")).isEqualTo("char_col"),
        () -> assertThat(row.getString("tinytext_column")).isEqualTo("tinytext_column_value"),
        () -> assertThat(row.getString("mediumtext_column")).isEqualTo("mediumtext_column_value"),
        () -> assertThat(row.getString("longtext_column")).isEqualTo("longtext_column_value"),
        () -> assertThat(row.getString("enum_column")).isEqualTo("2"),
        () -> assertThat(row.getBoolean("bool_column")).isFalse(),
        () -> assertThat(row.getBoolean("other_bool_column")).isTrue());
  }
}
