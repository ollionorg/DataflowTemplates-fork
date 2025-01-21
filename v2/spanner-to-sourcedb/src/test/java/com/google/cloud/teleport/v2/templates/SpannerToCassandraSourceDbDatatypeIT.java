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
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.beam.it.cassandra.CassandraResourceManager;
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
@TemplateIntegrationTest(SpannerToSourceDbCassandraIT.class)
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
  public static CassandraResourceManager cassandraResourceManager;
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

        cassandraResourceManager = CassandraResourceManager.builder(testName).build();

        createCassandraSchema(cassandraResourceManager, CASSANDRA_SCHEMA_FILE_RESOURCE);

        gcsResourceManager =
            GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
                .build();
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
    Mutation m =
        Mutation.newInsertOrUpdateBuilder(TABLE)
            .set("varchar_column")
            .to("value1")
            .set("tinyint_column")
            .to(10)
            .set("text_column")
            .to("text_column_value")
            .set("date_column")
            .to(Value.date(Date.fromYearMonthDay(2024, 05, 24)))
            .set("smallint_column")
            .to(50)
            .set("mediumint_column")
            .to(1000)
            .set("int_column")
            .to(50000)
            .set("bigint_column")
            .to(987654321)
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
            .set("tinyblob_column")
            .to(Value.bytes(ByteArray.copyFrom("tinyblob_column_value")))
            .set("blob_column")
            .to(Value.bytes(ByteArray.copyFrom("blob_column_value")))
            .set("mediumblob_column")
            .to(Value.bytes(ByteArray.copyFrom("mediumblob_column_value")))
            .set("longblob_column")
            .to(Value.bytes(ByteArray.copyFrom("longblob_column_value")))
            .set("enum_column")
            .to("2")
            .set("bool_column")
            .to(Value.bool(Boolean.FALSE))
            .set("other_bool_column")
            .to(Value.bool(Boolean.TRUE))
            .set("binary_column")
            .to(Value.bytes(ByteArray.copyFrom("binary_col")))
            .set("varbinary_column")
            .to(Value.bytes(ByteArray.copyFrom("varbinary")))
            .set("bit_column")
            .to(Value.bytes(ByteArray.copyFrom("a")))
            .build();
    spannerResourceManager.write(m);
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
    assertThat(rows).hasSize(1);
    assertAll(
        () -> assertThat(row.getString("varchar_column")).isEqualTo("value1"),
        () -> assertThat(row.getByte("tinyint_column")).isEqualTo((byte) 10),
        () -> assertThat(row.getString("text_column")).isEqualTo("text_column_value"),
        () ->
            assertThat(row.getLocalDate("date_column"))
                .isEqualTo(java.time.LocalDate.of(2024, 5, 24)),
        () -> assertThat(row.getShort("smallint_column")).isEqualTo((short) 50),
        () -> assertThat(row.getInt("mediumint_column")).isEqualTo(1000),
        () -> assertThat(row.getInt("int_column")).isEqualTo(50000),
        () -> assertThat(row.getLong("bigint_column")).isEqualTo(987654321L),
        () -> assertThat(row.getFloat("float_column")).isEqualTo(45.67f),
        () -> assertThat(row.getDouble("double_column")).isEqualTo(123.789),
        () -> assertThat(row.getBigDecimal("decimal_column")).isEqualTo(new BigDecimal("1234.56")),
        () ->
            assertThat(row.getInstant("datetime_column"))
                .isEqualTo(
                    java.time.LocalDateTime.of(2024, 2, 8, 8, 15, 30).toInstant(ZoneOffset.UTC)),
        () ->
            assertThat(row.getInstant("timestamp_column"))
                .isEqualTo(java.sql.Timestamp.valueOf("2024-02-08 08:15:30").toInstant()),
        () ->
            assertThat(row.getLocalTime("time_column"))
                .isEqualTo(java.time.LocalTime.of(14, 30, 0)),
        () -> assertThat(row.getString("year_column")).isEqualTo("2022"),
        () -> assertThat(row.getString("char_column")).isEqualTo("char_col"),
        () -> assertThat(row.getString("tinytext_column")).isEqualTo("tinytext_column_value"),
        () -> assertThat(row.getString("mediumtext_column")).isEqualTo("mediumtext_column_value"),
        () -> assertThat(row.getString("longtext_column")).isEqualTo("longtext_column_value"),
        () ->
            assertThat(row.getByte("tinyblob_column"))
                .isEqualTo("tinyblob_column_value".getBytes(StandardCharsets.UTF_8)),
        () ->
            assertThat(row.getByte("blob_column"))
                .isEqualTo("blob_column_value".getBytes(StandardCharsets.UTF_8)),
        () ->
            assertThat(row.getByte("mediumblob_column"))
                .isEqualTo("mediumblob_column_value".getBytes(StandardCharsets.UTF_8)),
        () ->
            assertThat(row.getByte("longblob_column"))
                .isEqualTo("longblob_column_value".getBytes(StandardCharsets.UTF_8)),
        () -> assertThat(row.getString("enum_column")).isEqualTo("2"),
        () -> assertThat(row.getBoolean("bool_column")).isEqualTo(false),
        () -> assertThat(row.getBoolean("other_bool_column")).isEqualTo(true),
        () ->
            assertThat(row.getByte("binary_column"))
                .isEqualTo("binary_col".getBytes(StandardCharsets.UTF_8)),
        () ->
            assertThat(row.getByte("varbinary_column"))
                .isEqualTo("varbinary".getBytes(StandardCharsets.UTF_8)),
        () ->
            assertThat(row.getBigInteger("bit_column"))
                .isEqualTo(new BigInteger("a".getBytes(StandardCharsets.UTF_8))));
  }
}
