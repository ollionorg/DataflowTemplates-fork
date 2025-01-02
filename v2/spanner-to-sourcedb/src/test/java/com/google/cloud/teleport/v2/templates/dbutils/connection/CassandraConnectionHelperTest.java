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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.google.cloud.teleport.v2.spanner.migrations.shard.CassandraShard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.templates.models.ConnectionHelperRequest;
import java.util.Arrays;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;

@RunWith(JUnit4.class)
public class CassandraConnectionHelperTest {

  @Mock private CassandraShard cassandraShard;
  @Mock private CassandraConnectionHelper connectionHelper;

  @Before
  public void setUp() {
    connectionHelper = new CassandraConnectionHelper();
    cassandraShard = mock(CassandraShard.class);
  }

  @Test
  public void testInit_ShouldInitializeConnectionPool() {
    when(cassandraShard.getHost()).thenReturn("localhost");
    when(cassandraShard.getPort()).thenReturn("9042");
    when(cassandraShard.getUserName()).thenReturn("user");
    when(cassandraShard.getPassword()).thenReturn("password");
    when(cassandraShard.getKeySpaceName()).thenReturn("mykeyspace");

    ConnectionHelperRequest request = mock(ConnectionHelperRequest.class);
    when(request.getShards()).thenReturn(Arrays.asList(cassandraShard));
    when(request.getMaxConnections()).thenReturn(10);
    connectionHelper.init(request);
    assertTrue(connectionHelper.isConnectionPoolInitialized());
  }

  @Test
  public void testGetConnection_ShouldReturnValidSession() throws ConnectionException {
    String connectionKey = "localhost:9042/user/mykeyspace";
    CqlSession mockSession = mock(CqlSession.class);
    connectionHelper.setConnectionPoolMap(Map.of(connectionKey, mockSession));

    CqlSession session = connectionHelper.getConnection(connectionKey);

    assertNotNull(session);
    assertEquals(mockSession, session);
  }

  @Test
  public void testGetConnection_ShouldThrowException_WhenConnectionNotFound() {
    assertThrows(
        ConnectionException.class,
        () -> {
          connectionHelper.getConnection("invalidKey");
        });
  }

  @Test
  public void testIsConnectionPoolInitialized_ShouldReturnTrue_WhenInitialized() {
    ConnectionHelperRequest request = mock(ConnectionHelperRequest.class);
    when(request.getShards()).thenReturn(Arrays.asList(mock(CassandraShard.class)));
    when(request.getMaxConnections()).thenReturn(10);

    connectionHelper.init(request);

    assertTrue(connectionHelper.isConnectionPoolInitialized());
  }

  @Test
  public void testGetConnection_ShouldThrowConnectionException_WhenPoolNotInitialized() {
    assertThrows(
        ConnectionException.class,
        () -> {
          connectionHelper.getConnection("anyKey");
        });
  }

  @Test
  public void testInit_ShouldHandleException_WhenCqlSessionCreationFails() {
    CassandraShard invalidShard = mock(CassandraShard.class);
    when(invalidShard.getHost()).thenReturn("localhost");
    when(invalidShard.getPort()).thenReturn("9042");
    when(invalidShard.getUserName()).thenReturn("invalidUser");
    when(invalidShard.getPassword()).thenReturn("invalidPassword");
    when(invalidShard.getKeySpaceName()).thenReturn("mykeyspace");

    ConnectionHelperRequest request = mock(ConnectionHelperRequest.class);
    when(request.getShards()).thenReturn(Arrays.asList(invalidShard));
    when(request.getMaxConnections()).thenReturn(10);

    connectionHelper.init(request);
    assertFalse(connectionHelper.isConnectionPoolInitialized());
  }

  @Test
  public void testSetConnectionPoolMap_ShouldOverrideConnectionPoolMap()
      throws ConnectionException {
    CqlSession mockSession = mock(CqlSession.class);
    connectionHelper.setConnectionPoolMap(Map.of("localhost:9042/user/mykeyspace", mockSession));

    CqlSession session = connectionHelper.getConnection("localhost:9042/user/mykeyspace");
    assertNotNull(session);
    assertEquals(mockSession, session);
  }

  @Test
  public void testGetConnectionPoolNotFound() {
    connectionHelper.setConnectionPoolMap(Map.of());

    ConnectionException exception =
        assertThrows(
            ConnectionException.class,
            () -> {
              connectionHelper.getConnection("nonexistentKey");
            });

    assertEquals("Connection pool is not initialized.", exception.getMessage());
  }

  @Test
  public void testGetConnectionWhenPoolNotInitialized() {
    connectionHelper.setConnectionPoolMap(null);
    ConnectionException exception =
        assertThrows(
            ConnectionException.class,
            () -> {
              connectionHelper.getConnection("localhost:9042/testuser/testKeyspace");
            });
    assertEquals("Connection pool is not initialized.", exception.getMessage());
  }

  @Test
  public void testGetConnectionWithValidKey() throws ConnectionException {
    CqlSession mockSession = mock(CqlSession.class);

    String connectionKey = "localhost:9042/testuser/testKeyspace";
    connectionHelper.setConnectionPoolMap(Map.of(connectionKey, mockSession));

    CqlSession session = connectionHelper.getConnection(connectionKey);

    assertEquals(mockSession, session, "The returned connection should match the mock session.");
  }

  @Test
  public void testInit_ShouldThrowIllegalArgumentException_WhenInvalidShardTypeIsProvideds() {
    Shard invalidShard = mock(Shard.class);
    CassandraConnectionHelper connectionHelper = new CassandraConnectionHelper();
    ConnectionHelperRequest request = mock(ConnectionHelperRequest.class);
    when(request.getShards()).thenReturn(java.util.Collections.singletonList(invalidShard));
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              connectionHelper.init(request);
            });
    assertEquals("Invalid shard object", exception.getMessage());
  }
}
