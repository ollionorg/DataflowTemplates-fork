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
package com.google.cloud.teleport.v2.templates.dbutils.dao.source;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.google.cloud.teleport.v2.templates.dbutils.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.templates.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementGeneratedResponse;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementValueObject;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CassandraDaoTest {

  @Mock private IConnectionHelper mockConnectionHelper;
  @Mock private CqlSession mockSession;
  @Mock private PreparedStatement mockPreparedStatement;
  @Mock private BoundStatement mockBoundStatement;
  @Mock private PreparedStatementGeneratedResponse mockPreparedStatementGeneratedResponse;

  private CassandraDao cassandraDao;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    cassandraDao = new CassandraDao("cassandraUrl", "cassandraUser", mockConnectionHelper);
  }

  @Test(expected = ConnectionException.class)
  public void testNullConnectionForWrite() throws Exception {
    when(mockConnectionHelper.getConnection(anyString())).thenReturn(null);
    cassandraDao.write(mockPreparedStatementGeneratedResponse);
  }

  @Test
  public void testPreparedStatementExecution() throws Exception {
    String preparedDmlStatement = "INSERT INTO test (id, name) VALUES (?, ?)";
    List<PreparedStatementValueObject<?>> values =
        Arrays.asList(
            new PreparedStatementValueObject<>("", preparedDmlStatement),
            new PreparedStatementValueObject<>("Test", preparedDmlStatement));

    when(mockPreparedStatementGeneratedResponse.getDmlStatement()).thenReturn(preparedDmlStatement);
    when(mockPreparedStatementGeneratedResponse.getValues()).thenReturn(values);
    when(mockConnectionHelper.getConnection(anyString())).thenReturn(mockSession);
    when(mockSession.prepare(eq(preparedDmlStatement))).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.bind(any())).thenReturn(mockBoundStatement);

    cassandraDao.write(mockPreparedStatementGeneratedResponse);

    verify(mockSession).prepare(eq(preparedDmlStatement));
    verify(mockPreparedStatement).bind(any());
    verify(mockSession).execute(eq(mockBoundStatement));
  }

  @Test
  public void testWriteWithExceptionInPreparedStatement() throws Exception {
    String preparedDmlStatement = "INSERT INTO test (id, name) VALUES (?, ?)";
    List<PreparedStatementValueObject<?>> values =
        Arrays.asList(
            new PreparedStatementValueObject<>("", preparedDmlStatement),
            new PreparedStatementValueObject<>("Test", preparedDmlStatement));

    when(mockPreparedStatementGeneratedResponse.getDmlStatement()).thenReturn(preparedDmlStatement);
    when(mockPreparedStatementGeneratedResponse.getValues()).thenReturn(values);
    when(mockConnectionHelper.getConnection(anyString())).thenReturn(mockSession);
    when(mockSession.prepare(eq(preparedDmlStatement))).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.bind(any())).thenReturn(mockBoundStatement);
    doThrow(new RuntimeException("Prepared statement execution failed"))
        .when(mockSession)
        .execute(eq(mockBoundStatement));

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> {
              cassandraDao.write(mockPreparedStatementGeneratedResponse);
            });

    assertEquals("Prepared statement execution failed", exception.getMessage());
    verify(mockSession).prepare(eq(preparedDmlStatement));
    verify(mockPreparedStatement).bind(any());
    verify(mockSession).execute(eq(mockBoundStatement));
  }

  @Test
  public void testWriteWithExceptionHandling() throws Exception {
    String dmlStatement = "INSERT INTO test (id, name) VALUES (?, ?)";
    when(mockPreparedStatementGeneratedResponse.getDmlStatement()).thenReturn(dmlStatement);
    when(mockConnectionHelper.getConnection(anyString())).thenReturn(mockSession);
    when(mockSession.prepare(dmlStatement))
        .thenThrow(new RuntimeException("Failed to prepare statement"));

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> {
              cassandraDao.write(mockPreparedStatementGeneratedResponse);
            });

    assertEquals("Failed to prepare statement", exception.getMessage());
    verify(mockSession).prepare(dmlStatement);
    verify(mockSession, never()).execute((Statement<?>) any());
  }

  @Test(expected = ConnectionException.class)
  public void testConnectionExceptionDuringWrite() throws Exception {
    when(mockConnectionHelper.getConnection(anyString()))
        .thenThrow(new ConnectionException("Connection failed"));
    cassandraDao.write(mockPreparedStatementGeneratedResponse);
  }

  @Test
  public void testWriteWithSimpleStatementExecution() throws Exception {
    String simpleStatement = "DELETE FROM test WHERE id = 1";
    DMLGeneratorResponse mockDmlGeneratorResponse = mock(DMLGeneratorResponse.class);

    when(mockDmlGeneratorResponse.getDmlStatement()).thenReturn(simpleStatement);
    when(mockConnectionHelper.getConnection(anyString())).thenReturn(mockSession);

    cassandraDao.write(mockDmlGeneratorResponse);

    verify(mockSession).execute(eq(simpleStatement));
    verify(mockSession, never()).prepare(anyString());
  }
}
