package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.metadata.TemplateLoadTest;
import org.apache.beam.it.cassandra.CassandraResourceManager;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.gcp.datagenerator.DataGenerator;
import org.apache.beam.it.jdbc.conditions.JDBCRowsCheck;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.io.Resources;

import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;

import static org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils.getFullGcsPath;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

@Category(TemplateLoadTest.class)
@TemplateLoadTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)

public class SpannerToCassandraSourceLT extends SpannerToCassandraLTBase {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerToCassandraSourceLT.class);

  private String generatorSchemaPath;
  private final String artifactBucket = TestProperties.artifactBucket();
  private final String spannerDdlResource = "SpannerToCassandraSourceLT/spanner-schema.sql";
  private final String sessionFileResource = "SpannerToCassandraSourceLT/session.json";
  private final String dataGeneratorSchemaResource = "SpannerToCassandraSourceLT/datagenerator-schema.json";
  private final String table = "Person";
  private final int maxWorkers = 50;
  private final int numWorkers = 20;
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
                    Resources.getResource(dataGeneratorSchemaResource).getPath()
                ).name()
        );

    createCassandraSchema(cassandraResourceManager);
    jobInfo = launchDataflowJob(artifactBucket, numWorkers, maxWorkers);
  }

  @After
  public void teardown(){
    cleanupResourceManagers();
  }

  @Test
  public void reverseReplication1KTpsLoadTest() throws IOException, ParseException, InterruptedException {
    // Start data generator
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaLocation(testName, generatorSchemaPath)
            .setQPS("1000")
            .setMessagesLimit(String.valueOf(300000))
            .setSpannerInstanceName(spannerResourceManager.getInstanceId())
            .setSpannerDatabaseName(spannerResourceManager.getDatabaseId())
            .setSpannerTableName(table)
            .setNumWorkers("50")
            .setMaxNumWorkers("100")
            .setSinkType("SPANNER")
            .setProjectId(project)
            .setBatchSizeBytes("0")
            .build();

    dataGenerator.execute(Duration.ofMinutes(90));
    assertThatPipeline(jobInfo).isRunning();

//    Todo Check for cassandra
//    JDBCRowsCheck check =
//        JDBCRowsCheck.builder(jdbcResourceManagers.get(0), table)
//            .setMinRows(300000)
//            .setMaxRows(300000)
//            .build();

    PipelineOperator.Result result =
        pipelineOperator.waitForCondition(
            createConfig(jobInfo, Duration.ofMinutes(10), Duration.ofSeconds(30)), check);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    PipelineOperator.Result result1 =
        pipelineOperator.cancelJobAndFinish(createConfig(jobInfo, Duration.ofMinutes(20)));

    assertThatResult(result1).isLaunchFinished();

    exportMetrics(jobInfo, numShards);
  }

  private void createCassandraSchema(CassandraResourceManager cassandraResourceManager){
    String keyspace = cassandraResourceManager.getKeyspaceName();
    String createTableStatement = String.format(
        "CREATE TABLE IF NOT EXISTS %s.%s ("
            + "ID uuid PRIMARY KEY, "
            + "first_name1 text, "
            + "last_name1 text, "
            + "first_name2 text, "
            + "last_name2 text, "
            + "first_name3 text, "
            + "last_name3 text);",
        keyspace, table
    );
    cassandraResourceManager.executeStatement(createTableStatement);
  }
}

