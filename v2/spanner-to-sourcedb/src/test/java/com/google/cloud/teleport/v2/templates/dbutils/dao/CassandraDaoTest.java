package com.google.cloud.teleport.v2.templates.dbutils.dao;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import com.datastax.oss.driver.api.core.cql.BoundStatement;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.google.cloud.teleport.v2.templates.dbutils.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.CassandraDao;
import com.google.cloud.teleport.v2.templates.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementGeneratedResponse;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.*;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.Arrays;
import java.util.List;

public class CassandraDaoTest {

    @Rule
    public final MockitoRule mockito = MockitoJUnit.rule();

    @Mock
    private IConnectionHelper mockConnectionHelper;

    @Mock
    private CqlSession mockSession;

    @Mock
    private PreparedStatement mockPreparedStatement;

    @Mock
    private BoundStatement mockBoundStatement;

    @Mock
    private ResultSet mockResultSet;

    @Mock
    private DMLGeneratorResponse mockDmlGeneratorResponse;

    @Mock
    private PreparedStatementGeneratedResponse mockPreparedStatementGeneratedResponse;

    private CassandraDao cassandraDao;

    @Before
    public void setup() throws Exception {
        cassandraDao = new CassandraDao("cassandraUrl", "user", mockConnectionHelper);
        when(mockConnectionHelper.getConnection(any())).thenReturn(mockSession);
    }

    // Test Case 1: Null Connection - Should throw ConnectionException
    @Test(expected = ConnectionException.class)
    public void testNullConnection() throws Exception {
        when(mockConnectionHelper.getConnection(any())).thenReturn(null);
        cassandraDao.execute(mockDmlGeneratorResponse);
    }

    @Test
    public void testExecuteSimpleStatement() throws Exception {
        String preparedDmlStatement = "INSERT INTO test (id, name) VALUES (?, ?)";
        List<Object> values = Arrays.asList(1, "Test");
        when(mockPreparedStatementGeneratedResponse.getDmlStatement()).thenReturn(preparedDmlStatement);
        when(mockPreparedStatementGeneratedResponse.getValues()).thenReturn(values);
        when(mockSession.prepare((SimpleStatement) any())).thenThrow(new RuntimeException("Prepared statement failed"));
        ResultSet resultSet = cassandraDao.execute(mockPreparedStatementGeneratedResponse);
        verify(mockSession).prepare(eq(preparedDmlStatement));
        verify(mockSession, never()).execute((Statement<?>) any());
    }

    @Test
    public void testExceptionDuringPreparedStatementExecution() throws Exception {
        String preparedDmlStatement = "INSERT INTO test (id, name) VALUES (?, ?)";
        List<Object> values = Arrays.asList(1, "Test");
        when(mockPreparedStatementGeneratedResponse.getDmlStatement()).thenReturn(preparedDmlStatement);
        when(mockPreparedStatementGeneratedResponse.getValues()).thenReturn(values);
        when(mockSession.prepare((SimpleStatement) any())).thenThrow(new RuntimeException("Prepared statement failed"));
        ResultSet resultSet = cassandraDao.execute(mockPreparedStatementGeneratedResponse);
        verify(mockSession).prepare(eq(preparedDmlStatement));
        verify(mockSession, never()).execute((Statement<?>) any());
    }
}
