/*
 * Copyright (C) 2024 Google LLC
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
import org.apache.beam.it.gcp.datastream.JDBCSource;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance tests for {@link DataStreamToSpanner} DataStream to Spanner template. */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
@Ignore
public class DataStreamToSpanner100GbFor10MbPerColumnTablesLT extends DataStreamToSpannerLTBase {
  private static final int NUM_TABLES = 1;
  private static final int RECORD_PER_TABLE = 6500000;
  private static final String SCHEMA_FILE =
      "DataStreamToSpanner100GbFor10MbPerColumnTablesLT/spanner-schema.sql";

  @Test
  public void backFill100GbDataFor5000Tables()
      throws IOException, ParseException, InterruptedException {
    setUpResourceManagers(SCHEMA_FILE);
    HashMap<String, Integer> tables100GB = new HashMap<>();
    for (int i = 1; i <= NUM_TABLES; i++) {
      tables100GB.put("person" + i, RECORD_PER_TABLE);
    }

    // Setup Datastream
    String hostIp =
        secretClient.accessSecret(
            "projects/269744978479/secrets/nokill-datastream-mysql-to-spanner-cloudsql-ip-address/versions/1");
    String username =
        secretClient.accessSecret(
            "projects/269744978479/secrets/nokill-datastream-mysql-to-spanner-cloudsql-username/versions/1");
    String password =
        secretClient.accessSecret(
            "projects/269744978479/secrets/nokill-datastream-mysql-to-spanner-cloudsql-password/versions/1");

    JDBCSource mySQLSource = getMySQLSource(hostIp, username, password);
    runLoadTest(tables100GB, mySQLSource);
  }
}
