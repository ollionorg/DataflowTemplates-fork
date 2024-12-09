/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.spanner.migrations.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.migrations.shard.MySqlShard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public final class MySqlShardFileReaderTest {
  @Rule public final MockitoRule mocktio = MockitoJUnit.rule();
  @Mock private ISecretManagerAccessor secretManagerAccessorMockImpl;

  @Test
  public void shardFileReading() {
    ShardFileReader shardFileReader = new ShardFileReader(new SecretManagerAccessorImpl());
    List<IShard> mySqlShards = shardFileReader.getOrderedShardDetails("src/test/resources/shard.json");
    List<MySqlShard> expectedMySqlShards =
        Arrays.asList(
            new MySqlShard(
                "shardA", "hostShardA", "3306", "test", "test", "test", "namespaceA", null, null),
            new MySqlShard("shardB", "hostShardB", "3306", "test", "test", "test", null, null, null));

    assertEquals(mySqlShards, expectedMySqlShards);
  }

  @Test
  public void shardFileReadingFileNotExists() {
    ShardFileReader shardFileReader = new ShardFileReader(new SecretManagerAccessorImpl());
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                shardFileReader.getOrderedShardDetails("src/test/resources/somemissingfile.json"));
    assertTrue(thrown.getMessage().contains("Failed to read shard input file"));
  }

  @Test
  public void shardFileReadingWithSecret() {

    when(secretManagerAccessorMockImpl.getSecret("projects/123/secrets/secretA/versions/latest"))
        .thenReturn("secretA");
    when(secretManagerAccessorMockImpl.getSecret("projects/123/secrets/secretB/versions/latest"))
        .thenReturn("secretB");
    when(secretManagerAccessorMockImpl.getSecret("projects/123/secrets/secretC/versions/latest"))
        .thenReturn("secretC");

    ShardFileReader shardFileReader = new ShardFileReader(secretManagerAccessorMockImpl);
    List<IShard> mySqlShards =
        shardFileReader.getOrderedShardDetails("src/test/resources/shard-with-secret.json");
    List<MySqlShard> expectedMySqlShards =
        Arrays.asList(
            new MySqlShard(
                "shardA",
                "hostShardA",
                "3306",
                "test",
                "secretA",
                "test",
                "namespaceA",
                "projects/123/secrets/secretA/versions/latest",
                null),
            new MySqlShard(
                "shardB",
                "hostShardB",
                "3306",
                "test",
                "secretB",
                "test",
                null,
                "projects/123/secrets/secretB",
                null),
            new MySqlShard(
                "shardC",
                "hostShardC",
                "3306",
                "test",
                "secretC",
                "test",
                "namespaceC",
                "projects/123/secrets/secretC/",
                null),
            new MySqlShard("shardD", "hostShardD", "3306", "test", "test", "test", null, null, null));

    assertEquals(mySqlShards, expectedMySqlShards);
  }

  @Test
  public void shardFileSecretPatternIncorrect() {
    ShardFileReader shardFileReader = new ShardFileReader(new SecretManagerAccessorImpl());
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                shardFileReader.getOrderedShardDetails(
                    "src/test/resources/shard-with-secret-error.json"));
    assertTrue(
        thrown
            .getMessage()
            .contains("does not adhere to expected pattern projects/.*/secrets/.*/versions/.*"));
  }

  @Test
  public void shardFileWithNoCredentials() {
    ShardFileReader shardFileReader = new ShardFileReader(new SecretManagerAccessorImpl());
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                shardFileReader.getOrderedShardDetails(
                    "src/test/resources/shard-with-nocreds.json"));
    assertTrue(
        thrown
            .getMessage()
            .contains("Neither password nor secretManagerUri was found in the shard file"));
  }

  @Test
  public void readBulkMigrationShardFile() {
    String testConnectionProperties =
        "useUnicode=yes&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull";
    ShardFileReader shardFileReader = new ShardFileReader(new SecretManagerAccessorImpl());
    List<IShard> mySqlShards =
        shardFileReader.readForwardMigrationShardingConfig(
            "src/test/resources/bulk-migration-mySqlShards.json");
    MySqlShard mySqlShard1 =
        new MySqlShard(
            "",
            "1.1.1.1",
            "3306",
            "test1",
            "pass1",
            "",
            "namespace1",
            null,
            testConnectionProperties);
    mySqlShard1.getDbNameToLogicalShardIdMap().put("person1", "1-1-1-1-person");
    mySqlShard1.getDbNameToLogicalShardIdMap().put("person2", "1-1-1-1-person2");
    MySqlShard mySqlShard2 = new MySqlShard("", "1.1.1.2", "3306", "test1", "pass1", "", null, null, "");
    mySqlShard2.getDbNameToLogicalShardIdMap().put("person1", "1-1-1-2-person");
    mySqlShard2.getDbNameToLogicalShardIdMap().put("person20", "1-1-1-2-person2");
    List<MySqlShard> expectedMySqlShards = new ArrayList<>(Arrays.asList(mySqlShard1, mySqlShard2));

    assertEquals(expectedMySqlShards, mySqlShards);
    assertEquals(mySqlShard1.toString().contains(testConnectionProperties), true);
    assertEquals(mySqlShard1.getConnectionProperties(), testConnectionProperties);
    var originalHarshCode = mySqlShard1.hashCode();
    mySqlShard1.setConnectionProperties("");
    assertNotEquals(originalHarshCode, mySqlShard1.hashCode());
    // Cover the equality override.
    assertEquals(mySqlShard1, mySqlShard1);
    assertNotEquals(mySqlShard1, "");
    assertNotEquals(mySqlShard1, mySqlShards.get(0));
  }

  @Test
  public void readBulkMigrationShardFileWithSecrets() {
    when(secretManagerAccessorMockImpl.getSecret("projects/123/secrets/secretA/versions/latest"))
        .thenReturn("secretA");
    when(secretManagerAccessorMockImpl.getSecret("projects/123/secrets/secretB/versions/latest"))
        .thenReturn("secretB");
    ShardFileReader shardFileReader = new ShardFileReader(secretManagerAccessorMockImpl);
    List<IShard> mySqlShards =
        shardFileReader.readForwardMigrationShardingConfig(
            "src/test/resources/bulk-migration-mySqlShards-secret.json");
    MySqlShard mySqlShard1 =
        new MySqlShard(
            "",
            "1.1.1.1",
            "3306",
            "test1",
            "secretA",
            "",
            null,
            "projects/123/secrets/secretA/versions/latest",
            "");
    mySqlShard1.getDbNameToLogicalShardIdMap().put("person1", "1-1-1-1-person");
    mySqlShard1.getDbNameToLogicalShardIdMap().put("person2", "1-1-1-1-person2");
    MySqlShard mySqlShard2 =
        new MySqlShard(
            "",
            "1.1.1.2",
            "3306",
            "test1",
            "secretB",
            "",
            null,
            "projects/123/secrets/secretB/versions/latest",
            "");
    mySqlShard2.getDbNameToLogicalShardIdMap().put("person1", "1-1-1-2-person");
    mySqlShard2.getDbNameToLogicalShardIdMap().put("person20", "1-1-1-2-person2");
    List<MySqlShard> expectedMySqlShards = new ArrayList<>(Arrays.asList(mySqlShard1, mySqlShard2));

    assertEquals(mySqlShards, expectedMySqlShards);
  }
}
