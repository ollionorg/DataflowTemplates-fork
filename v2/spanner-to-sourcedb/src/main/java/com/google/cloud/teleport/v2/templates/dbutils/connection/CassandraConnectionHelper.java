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
package com.google.cloud.teleport.v2.templates.dbutils.connection;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.google.cloud.teleport.v2.spanner.migrations.shard.CassandraShard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.templates.models.ConnectionHelperRequest;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code CassandraConnectionHelper} class provides methods to manage and maintain connections
 * to a Cassandra database in a multi-shard environment. It implements the {@link IConnectionHelper}
 * interface for {@link CqlSession}.
 *
 * <p>This class initializes and maintains a connection pool for multiple Cassandra shards and
 * provides utilities to retrieve connections based on a unique key.
 *
 * <p>Typical usage:
 *
 * <pre>
 *   CassandraConnectionHelper helper = new CassandraConnectionHelper();
 *   helper.init(connectionHelperRequest);
 *   CqlSession session = helper.getConnection(connectionKey);
 * </pre>
 */
public class CassandraConnectionHelper implements IConnectionHelper<CqlSession> {

  /** Logger for logging information and errors. */
  private static final Logger LOG = LoggerFactory.getLogger(CassandraConnectionHelper.class);

  /** A thread-safe connection pool storing {@link CqlSession} instances mapped to unique keys. */
  private static Map<String, CqlSession> connectionPoolMap = new ConcurrentHashMap<>();

  /**
   * Initializes the connection pool with connections for the provided Cassandra shards.
   *
   * @param connectionHelperRequest The request object containing shard details and connection
   *     settings.
   * @throws IllegalArgumentException if any shard validation fails or invalid shard types are
   *     provided.
   */
  @Override
  public synchronized void init(ConnectionHelperRequest connectionHelperRequest) {
    if (connectionPoolMap != null && !connectionPoolMap.isEmpty()) {
      LOG.info("Connection pool is already initialized.");
      return;
    }

    LOG.info(
        "Initializing Cassandra connection pool with size: {}",
        connectionHelperRequest.getMaxConnections());

    List<Shard> shards = connectionHelperRequest.getShards();
    for (Shard shard : shards) {
      if (!(shard instanceof CassandraShard)) {
        LOG.error("Invalid shard type: {}", shard.getClass().getSimpleName());
        throw new IllegalArgumentException("Invalid shard object");
      }

      CassandraShard cassandraShard = (CassandraShard) shard;
      try {
        cassandraShard.validate();
        CqlSession session = createCqlSession(cassandraShard);
        String connectionKey = generateConnectionKey(cassandraShard);
        connectionPoolMap.put(connectionKey, session);
        LOG.info("Connection initialized for key: {}", connectionKey);
      } catch (Exception e) {
        LOG.error("Failed to initialize connection for shard: {}", cassandraShard, e);
      }
    }
  }

  /**
   * Retrieves a {@link CqlSession} connection from the connection pool.
   *
   * @param connectionRequestKey The unique key identifying the connection in the pool.
   * @return The {@link CqlSession} instance associated with the given key.
   * @throws ConnectionException If the connection pool is not initialized or no connection is found
   *     for the key.
   */
  @Override
  public CqlSession getConnection(String connectionRequestKey) throws ConnectionException {
    if (connectionPoolMap == null || connectionPoolMap.isEmpty()) {
      LOG.warn("Connection pool not initialized.");
      throw new ConnectionException("Connection pool is not initialized.");
    }

    CqlSession session = connectionPoolMap.get(connectionRequestKey);
    if (session == null) {
      LOG.warn("No connection found for key: {}", connectionRequestKey);
      throw new ConnectionException(
          "No connection available for the given key: " + connectionRequestKey);
    }

    return session;
  }

  /**
   * Checks if the connection pool is initialized and contains connections.
   *
   * @return {@code true} if the connection pool is initialized and not empty; {@code false}
   *     otherwise.
   */
  @Override
  public boolean isConnectionPoolInitialized() {
    return connectionPoolMap != null && !connectionPoolMap.isEmpty();
  }

  /**
   * Creates a {@link CqlSession} for the given {@link CassandraShard}.
   *
   * @param cassandraShard The shard containing connection details.
   * @return A {@link CqlSession} instance.
   */
  private CqlSession createCqlSession(CassandraShard cassandraShard) {
    CqlSessionBuilder builder =
        CqlSession.builder()
            .addContactPoint(
                new InetSocketAddress(
                    cassandraShard.getHost(), Integer.parseInt(cassandraShard.getPort())))
            .withAuthCredentials(cassandraShard.getUserName(), cassandraShard.getPassword())
            .withKeyspace(cassandraShard.getKeySpaceName());

    DriverConfigLoader configLoader = createConfigLoader(cassandraShard);
    builder.withConfigLoader(configLoader);

    return builder.build();
  }

  /**
   * Generates a unique connection key for the given {@link CassandraShard}.
   *
   * @param shard The shard containing connection details.
   * @return A string key uniquely identifying the connection.
   */
  private String generateConnectionKey(CassandraShard shard) {
    return String.format(
        "%s:%s/%s/%s",
        shard.getHost(), shard.getPort(), shard.getUserName(), shard.getKeySpaceName());
  }

  /**
   * Creates a driver configuration loader for the given {@link CassandraShard}.
   *
   * @param cassandraShard The shard containing configuration details.
   * @return A {@link DriverConfigLoader} instance.
   */
  private DriverConfigLoader createConfigLoader(CassandraShard cassandraShard) {
    ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder =
        DriverConfigLoader.programmaticBuilder();

    configLoaderBuilder
        .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, cassandraShard.getLocalPoolSize())
        .withInt(
            DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, cassandraShard.getRemotePoolSize());

    return configLoaderBuilder.build();
  }

  /**
   * Sets the connection pool for testing purposes.
   *
   * @param inputMap A map containing pre-configured connections for testing.
   */
  public void setConnectionPoolMap(Map<String, CqlSession> inputMap) {
    connectionPoolMap = inputMap;
  }
}