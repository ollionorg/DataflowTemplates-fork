/*
 * Copyright (C) 2022 Google LLC
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

import static com.google.cloud.teleport.it.common.TestProperties.getProperty;
import static com.google.cloud.teleport.it.gcp.bigquery.BigQueryResourceManagerUtils.toTableSpec;
import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.common.PipelineOperator.Result;
import com.google.cloud.teleport.it.common.TestProperties;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.TemplateLoadTestBase;
import com.google.cloud.teleport.it.gcp.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.gcp.bigquery.conditions.BigQueryRowsCheck;
import com.google.cloud.teleport.it.gcp.datagenerator.DataGenerator;
import com.google.cloud.teleport.it.gcp.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.common.base.MoreObjects;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.Collections;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance tests for {@link PubSubToBigQuery PubSub to BigQuery} template. */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(PubSubToBigQuery.class)
@RunWith(JUnit4.class)
public class PubsubToBigQueryLT extends TemplateLoadTestBase {

  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(), "gs://dataflow-templates/latest/flex/PubSub_to_BigQuery_Flex");
  // 35,000,000 messages of the given schema make up approximately 10GB
  private static final int NUM_MESSAGES = 35_000_000;
  // schema should match schema supplied to generate fake records.
  private static final Schema SCHEMA =
      Schema.of(
          Field.of("eventId", StandardSQLTypeName.STRING),
          Field.of("eventTimestamp", StandardSQLTypeName.INT64),
          Field.of("ipv4", StandardSQLTypeName.STRING),
          Field.of("ipv6", StandardSQLTypeName.STRING),
          Field.of("country", StandardSQLTypeName.STRING),
          Field.of("username", StandardSQLTypeName.STRING),
          Field.of("quest", StandardSQLTypeName.STRING),
          Field.of("score", StandardSQLTypeName.INT64),
          Field.of("completed", StandardSQLTypeName.BOOL));
  private static final String INPUT_PCOLLECTION =
      "ReadPubSubSubscription/PubsubUnboundedSource.out0";
  private static final String OUTPUT_PCOLLECTION =
      "WriteSuccessfulRecords/StreamingInserts/StreamingWriteTables/StripShardId/Map.out0";
  private static PubsubResourceManager pubsubResourceManager;
  private static BigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setup() throws IOException {
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT)
            .credentialsProvider(CREDENTIALS_PROVIDER)
            .build();
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT).setCredentials(CREDENTIALS).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager, bigQueryResourceManager);
  }

  @Test
  public void testBacklog10gb() throws IOException, ParseException, InterruptedException {
    testBacklog(this::disableRunnerV2);
  }

  @Test
  public void testSteadyState1hr() throws ParseException, IOException, InterruptedException {
    testSteadyState1hr(this::disableRunnerV2);
  }

  @Test
  public void testSteadyState1hrUsingStreamingEngine()
      throws ParseException, IOException, InterruptedException {
    testSteadyState1hr(this::enableStreamingEngine);
  }

  @Ignore("RunnerV2 is disabled on streaming templates.")
  @Test
  public void testSteadyState1hrUsingRunnerV2()
      throws ParseException, IOException, InterruptedException {
    testSteadyState1hr(this::enableRunnerV2);
  }

  public void testBacklog(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, ParseException, InterruptedException {
    TopicName backlogTopic = pubsubResourceManager.createTopic("backlog-input");
    SubscriptionName backlogSubscription =
        pubsubResourceManager.createSubscription(backlogTopic, "backlog-subscription");
    // Generate fake data to topic
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
            .setQPS("1000000")
            .setMessagesLimit(String.valueOf(NUM_MESSAGES))
            .setTopic(backlogTopic.toString())
            .setNumWorkers("50")
            .setMaxNumWorkers("100")
            .build();
    dataGenerator.execute(Duration.ofMinutes(30));
    TableId table = bigQueryResourceManager.createTable(testName, SCHEMA);
    LaunchConfig options =
        paramsAdder
            .apply(
                LaunchConfig.builder(testName, SPEC_PATH)
                    .addEnvironment("maxWorkers", 5)
                    .addEnvironment("numWorkers", 4)
                    .addParameter("inputSubscription", backlogSubscription.toString())
                    .addParameter("outputTableSpec", toTableSpec(PROJECT, table))
                    .addParameter("useStorageWriteApi", "true")
                    .addParameter("numStorageWriteApiStreams", "20")
                    .addParameter("storageWriteApiTriggeringFrequencySec", "60")
                    .addParameter("autoscalingAlgorithm", "THROUGHPUT_BASED"))
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options);
    assertThatPipeline(info).isRunning();
    Result result =
        pipelineOperator.waitForConditionAndCancel(
            createConfig(info, Duration.ofMinutes(40)),
            BigQueryRowsCheck.builder(bigQueryResourceManager, table)
                .setMinRows(NUM_MESSAGES)
                .build());

    // Assert
    assertThatResult(result).meetsConditions();

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }

  public void testSteadyState1hr(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws ParseException, IOException, InterruptedException {
    // Arrange
    String qps = getProperty("qps", "100000", TestProperties.Type.PROPERTY);
    TopicName inputTopic = pubsubResourceManager.createTopic("steady-state-input");
    SubscriptionName inputSubscription =
        pubsubResourceManager.createSubscription(inputTopic, "steady-state-subscription");
    TableId table =
        bigQueryResourceManager.createTable(
            testName, SCHEMA, System.currentTimeMillis() + 7200000); // expire in 2 hrs
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
            .setQPS(qps)
            .setTopic(inputTopic.toString())
            .setNumWorkers("8")
            .setMaxNumWorkers("10")
            .build();

    LaunchConfig options =
        paramsAdder
            .apply(
                LaunchConfig.builder(testName, SPEC_PATH)
                    .addEnvironment("maxWorkers", 8)
                    .addEnvironment("numWorkers", 7)
                    .addEnvironment("additionalUserLabels", Collections.singletonMap("qps", qps))
                    .addParameter("inputSubscription", inputSubscription.toString())
                    .addParameter("useStorageWriteApi", "true")
                    .addParameter("numStorageWriteApiStreams", "100")
                    .addParameter("storageWriteApiTriggeringFrequencySec", "60")
                    .addParameter("outputTableSpec", toTableSpec(PROJECT, table)))
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options);
    assertThatPipeline(info).isRunning();
    // ElementCount metric in dataflow is approximate, allow for 1% difference
    int expectedMessages = (int) (dataGenerator.execute(Duration.ofMinutes(60)) * 0.99);
    Result result =
        pipelineOperator.waitForConditionAndCancel(
            createConfig(info, Duration.ofMinutes(20)),
            BigQueryRowsCheck.builder(bigQueryResourceManager, table)
                .setMinRows(expectedMessages)
                .build());
    // Assert
    assertThatResult(result).meetsConditions();

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }
}