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

import com.datastax.oss.driver.api.core.DriverTimeoutException;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;
import org.apache.beam.it.cassandra.CassandraResourceManager;
import org.apache.beam.it.common.utils.ExceptionUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for managing Cassandra resources.
 *
 * <p>The class supports one database and multiple collections per database object. A database is
 * created when the first collection is created if one has not been created already.
 *
 * <p>The database name is formed using testId. The database name will be "{testId}-{ISO8601 time,
 * microsecond precision}", with additional formatting and handling restriction of keyspace Name.
 *
 * <p>The class is thread-safe.
 */
public class CassandraSharedResourceManager {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraSharedResourceManager.class);
  private CassandraResourceManager cassandraResourceManager;
  private final String keyspaceName;
  private final boolean usingStaticDatabase;

  private CassandraSharedResourceManager(Builder builder) {
    this.usingStaticDatabase = builder.keyspaceName != null && !builder.preGeneratedKeyspaceName;
    this.keyspaceName =
        usingStaticDatabase || builder.preGeneratedKeyspaceName
            ? builder.keyspaceName
            : generateKeyspaceName(builder.testId);
    cassandraResourceManager =
        CassandraResourceManager.builder(builder.testId).setKeyspaceName(this.keyspaceName).build();
    if (!usingStaticDatabase) {
      String keySpaceGeneratedDDL =
          String.format(
              "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}",
              this.keyspaceName);
      try {
        cassandraResourceManager.executeStatement(keySpaceGeneratedDDL);
      } catch (Exception e) {
        LOG.warn("Suppressing the error as it will always comes in case of Create Keysapce Name");
      }
    }
  }

  private String generateKeyspaceName(String testName) {
    return ResourceManagerUtils.generateResourceId(
            testName,
            Pattern.compile("[/\\\\. \"\u0000$]"),
            "-",
            27,
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSSSSS"))
        .replace('-', '_');
  }

  public static Builder builder(String testId) {
    return new Builder(testId);
  }

  /** Returns the port to connect to the Cassandra Database. */
  public int getPort() {
    return this.cassandraResourceManager.getPort();
  }

  /**
   * Returns the name of the Database that this Cassandra manager will operate in.
   *
   * @return the name of the Cassandra Database.
   */
  public synchronized String getKeyspaceName() {
    return keyspaceName;
  }

  public synchronized void cleanupAll() {
    LOG.info("Attempting to cleanup Cassandra manager.");

    boolean producedError = false;

    // First, delete the database if it was not given as a static argument
    if (!usingStaticDatabase) {
      try {
        String dropKeyspaceDDL = String.format("DROP KEYSPACE IF EXISTS %s", this.keyspaceName);
        this.cassandraResourceManager.executeStatement(dropKeyspaceDDL);
      } catch (Exception e) {
        LOG.error("Failed to drop Cassandra keyspace {}.", keyspaceName, e);
        if (!ExceptionUtils.containsType(e, DriverTimeoutException.class)
            && !ExceptionUtils.containsMessage(e, "does not exist")) {
          producedError = true;
        }
      }
    }
    // Throw Exception at the end if there were any errors
    if (producedError) {
      throw new IllegalArgumentException("Failed to delete resources. Check above for errors.");
    }
    // Next, try to close the Cassandra client connection
    this.cassandraResourceManager.cleanupAll();
    LOG.info("Cassandra manager successfully cleaned up.");
  }

  public CassandraResourceManager getCassandraResourceManager() {
    return this.cassandraResourceManager;
  }

  public static final class Builder {
    private @Nullable String testId;
    private @Nullable String keyspaceName;
    private @Nullable boolean preGeneratedKeyspaceName;

    private Builder(String testId) {
      this.testId = testId;
      this.keyspaceName = null;
    }

    /**
     * Sets the keyspace name to that of a preGeneratedKeyspaceName database instance.
     *
     * <p>Note: if a database name is set, and a static Cassandra server is being used
     * (useStaticContainer() is also called on the builder), then a database will be created on the
     * static server if it does not exist, and it will not be removed when cleanupAll() is called on
     * the CassandraResourceManager.
     *
     * @param keyspaceName The database name.
     * @return this builder object with the database name set.
     */
    public Builder setKeyspaceName(String keyspaceName) {
      this.keyspaceName = keyspaceName;
      return this;
    }

    /**
     * Sets the preGeneratedKeyspaceName to that of a static database instance. Use this method only
     * when attempting to operate on a pre-existing Cassandra database.
     *
     * <p>Note: if a database name is set, and a static Cassandra server is being used
     * (useStaticContainer() is also called on the builder), then a database will be created on the
     * static server if it does not exist, and it will not be removed when cleanupAll() is called on
     * the CassandraResourceManager.
     *
     * @param preGeneratedKeyspaceName The database name.
     * @return this builder object with the database is need to create.
     */
    public Builder sePreGeneratedKeyspaceName(boolean preGeneratedKeyspaceName) {
      this.preGeneratedKeyspaceName = preGeneratedKeyspaceName;
      return this;
    }

    public CassandraSharedResourceManager build() {
      return new CassandraSharedResourceManager(this);
    }
  }
}
