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

import static org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils.getFullGcsPath;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.TemplateLoadTest;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.gcp.datagenerator.DataGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.io.Resources;

@Category(TemplateLoadTest.class)
@TemplateLoadTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToCassandraSourceLT extends SpannerToCassandraLTBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToMySqlSourceLT.class);
  private String generatorSchemaPath;
  private final String artifactBucket = TestProperties.artifactBucket();
  private final String spannerDdlResource = "SpannerToCassandraSourceLT/spanner-schema.sql";
  private final String sessionFileResource = "SpannerToCassandraSourceLT/session.json";
  private final String dataGeneratorSchemaResource =
      "SpannerToCassandraSourceLT/datagenerator-schema.json";
  private final String table = "person";
  private final int maxWorkers = 3; // 50;
  private final int numWorkers = 2; // 20;
  private PipelineLauncher.LaunchInfo jobInfo;
  private PipelineLauncher.LaunchInfo readerJobInfo;
  private final int numShards = 1;

  @Before
  public void setup() throws IOException {
    setupResourceManagers(spannerDdlResource, sessionFileResource, artifactBucket);
    setupCassandraResourceManager();
    generatorSchemaPath =
        getFullGcsPath(
            artifactBucket,
            gcsResourceManager
                .uploadArtifact(
                    "input/schema.json",
                    Resources.getResource(dataGeneratorSchemaResource).getPath())
                .name());

    createCassandraSchema(cassandraResourceManager);
    jobInfo = launchDataflowJob(artifactBucket, numWorkers, maxWorkers);
  }

  @After
  public void teardown() {
    cleanupResourceManagers();
  }

  @Test
  public void reverseReplicationCassandra1KTpsLoadTest()
      throws IOException, ParseException, InterruptedException {
    // Start data generator
    LOG.info("SpannerInstanceID: {}", spannerResourceManager.getInstanceId());
    LOG.info("SpannerDatabaseID: {}", spannerResourceManager.getDatabaseId());
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaLocation(testName, generatorSchemaPath)
            .setQPS("100") // 1000
            .setMessagesLimit(String.valueOf(20)) // 300000
            .setSpannerInstanceName(spannerResourceManager.getInstanceId())
            .setSpannerDatabaseName(spannerResourceManager.getDatabaseId())
            .setSpannerTableName(table)
            .setNumWorkers("2") // 50
            .setMaxNumWorkers("3") // 100
            .setSinkType("SPANNER")
            .setProjectId(project)
            .setBatchSizeBytes("0")
            .build();

    dataGenerator.execute(Duration.ofMinutes(10)); // 90
    assertThatPipeline(jobInfo).isRunning();

    String tablename = cassandraResourceManager.getKeyspaceName() + ".person";
    CassandraRowsCheck check =
        CassandraRowsCheck.builder(cassandraResourceManager, tablename)
            .setMinRows(20) // 300000
            .setMaxRows(20) // 300000
            .build();

    PipelineOperator.Result result =
        pipelineOperator.waitForCondition(
            createConfig(jobInfo, Duration.ofMinutes(10), Duration.ofSeconds(30)), check);

    LOG.info("Job wait: {}", result);
    // Assert Conditions
    assertThatResult(result).meetsConditions();

    PipelineOperator.Result result1 =
        pipelineOperator.cancelJobAndFinish(createConfig(jobInfo, Duration.ofMinutes(20)));

    LOG.info("Job finished: {}", result1);
    assertThatResult(result1).isLaunchFinished();

    exportMetrics(jobInfo, numShards);
  }

  private void createCassandraSchema(CassandraSharedResourceManager cassandraResourceManager) {
    String keyspace = cassandraResourceManager.getKeyspaceName();
    String createTableStatement =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s ("
                + "id uuid PRIMARY KEY, "
                + "first_name1 text, "
                + "last_name1 text, "
                + "first_name2 text, "
                + "last_name2 text, "
                + "first_name3 text, "
                + "last_name3 text);",
            keyspace, table);

    LOG.info("Creating table: {}", createTableStatement);
    cassandraResourceManager.executeStatement(createTableStatement);
    LOG.info("Table Created");
  }
}
