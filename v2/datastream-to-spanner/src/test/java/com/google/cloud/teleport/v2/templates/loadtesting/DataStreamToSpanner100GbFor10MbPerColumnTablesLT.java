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
package com.google.cloud.teleport.v2.templates.loadtesting;

import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.gcp.datastream.JDBCSource;
import org.apache.beam.it.gcp.datastream.MySQLSource;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance tests for {@link DataStreamToSpanner} DataStream to Spanner template. */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpanner100GbFor10MbPerColumnTablesLT extends DataStreamToSpannerLTBase {
  private static final int RECORDS_PER_TABLE = 10_000;
  private static final String WORKER_MACHINE_TYPE = "n1-highmem-96";
  private static final String SCHEMA_FILE =
      "DataStreamToSpanner100GbFor10MbPerColumnTablesLT/spanner-schema.sql";
  private static final String DATABASE_NAME = "10MBStringCell";
  private static final String TABLE_NAME = "WideRowTable";

  private static final String PROJECT_ID = "209835939752";
  private static final String HOST_IP_SECRET = "sourcedb-mysql-to-spanner-cloudsql-ip-address";
  private static final String USERNAME_SECRET = "sourcedb-mysql-to-spanner-cloudsql-username";
  private static final String PASSWORD_SECRET = "sourcedb-mysql-to-spanner-cloudsql-password";
  private static final String SECRET_VERSION = "1";

  @Test
  public void backFill100GbDataFor10MBTables()
      throws IOException, ParseException, InterruptedException {
    setUpResourceManagers(SCHEMA_FILE);
    setUpResourceManagers(SCHEMA_FILE);
    HashMap<String, Integer> tables100GB = createTableConfiguration();
    JDBCSource mySQLSource = setupDatastreamConnection();
    HashMap<String, String> templateParameterOptions =
        new HashMap<>() {
          {
            put("workerMachineType", WORKER_MACHINE_TYPE);
          }
        };
    runLoadTest(tables100GB, mySQLSource, templateParameterOptions, new HashMap<>());
  }

  /**
   * Creates the table configuration for the load test.
   *
   * @return Map of table names to record counts
   */
  private HashMap<String, Integer> createTableConfiguration() {
    HashMap<String, Integer> tables = new HashMap<>();
    tables.put(TABLE_NAME, RECORDS_PER_TABLE);
    return tables;
  }

  /**
   * Sets up the Datastream connection using secrets from Secret Manager.
   *
   * @return Configured MySQL source
   * @throws IOException if secrets cannot be accessed
   */
  private JDBCSource setupDatastreamConnection() throws IOException {
    try {
      String hostIp = accessSecret(HOST_IP_SECRET);
      String username = accessSecret(USERNAME_SECRET);
      String password = accessSecret(PASSWORD_SECRET);
      Map<String, List<String>> allowedTables = new HashMap<>();
      allowedTables.computeIfAbsent(DATABASE_NAME, k -> new LinkedList<>()).add(TABLE_NAME);
      return createMySQLSource(hostIp, username, password, allowedTables);
    } catch (Exception e) {
      throw new IOException("Failed to setup Data stream connection", e);
    }
  }

  /**
   * Creates a MySQL source with the specified configuration.
   *
   * @param hostIp The host IP address
   * @param username The database username
   * @param password The database password
   * @param allowedTables Map of allowed tables
   * @return Configured MySQL source
   */
  private JDBCSource createMySQLSource(
      String hostIp, String username, String password, Map<String, List<String>> allowedTables) {
    return new MySQLSource.Builder(hostIp, username, password, 3306)
        .setAllowedTables(allowedTables)
        .build();
  }

  /**
   * Accesses a secret from Secret Manager.
   *
   * @param secretId The ID of the secret to access
   * @return The secret value
   * @throws IOException if the secret cannot be accessed
   */
  private String accessSecret(String secretId) throws IOException {
    try {
      String secretName =
          String.format("projects/%s/secrets/%s/versions/%s", PROJECT_ID, secretId, SECRET_VERSION);
      return secretClient.accessSecret(secretName);
    } catch (Exception e) {
      throw new IOException("Failed to access secret: " + secretId, e);
    }
  }
}
