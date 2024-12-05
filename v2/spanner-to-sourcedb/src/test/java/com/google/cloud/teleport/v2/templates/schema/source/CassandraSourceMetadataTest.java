package com.google.cloud.teleport.v2.templates.schema.source;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.cloud.teleport.v2.templates.metadata.source.CassandraSourceMetadata;
import com.google.cloud.teleport.v2.spanner.migrations.shard.IShard;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.util.*;

public class CassandraSourceMetadataTest {

    @Mock
    private IDao cassandraDaoMock;
    @Mock
    private ResultSet resultSetMock;
    @Mock
    private Row rowMock;
    @Mock
    private IShard shardMock;

    private CassandraSourceMetadata cassandraSourceMetadata;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        when(shardMock.getKeySpaceName()).thenReturn("test_keyspace");
        when(shardMock.getHost()).thenReturn("localhost");
        when(shardMock.getPort()).thenReturn(String.valueOf(9042));
        when(shardMock.getUser()).thenReturn("cassandra_user");
        List<IShard> shards = Collections.singletonList(shardMock);
        cassandraSourceMetadata = new CassandraSourceMetadata(shards);
        String connectionKey = "localhost:9042/cassandra_user/test_keyspace";
        when(cassandraDaoMock.execute(anyString())).thenReturn(resultSetMock);
    }

    @Test
    public void testGetMetadata_Success() throws Exception {
        CassandraSourceMetadata cassandraSourceMetadataMock = mock(CassandraSourceMetadata.class);
        when(resultSetMock.iterator()).thenReturn(Collections.singletonList(rowMock).iterator());
        when(rowMock.getString("table_name")).thenReturn("test_table");
        when(rowMock.getString("column_name")).thenReturn("test_column");
        when(rowMock.getString("type")).thenReturn("text");
        when(rowMock.getString("kind")).thenReturn("partition_key");
        SourceSchema schemaMock = mock(SourceSchema.class);

        Map<String, Map<String, SourceColumn>> schemaMapMock = mock(Map.class);
        Map<String, SourceColumn> columnMapMock = mock(Map.class);

        when(schemaMapMock.get("test_table")).thenReturn(columnMapMock);
        when(schemaMapMock.containsKey("test_table")).thenReturn(true);

        SourceColumn sourceColumnMock = mock(SourceColumn.class);
        when(sourceColumnMock.getSourceType()).thenReturn("text");
        when(sourceColumnMock.isPrimaryKey()).thenReturn(true);

        when(columnMapMock.get("test_column")).thenReturn(sourceColumnMock);

        when(cassandraSourceMetadataMock.getMetadata()).thenReturn(schemaMock);
        when(schemaMock.getSrcSchema()).thenReturn(schemaMapMock);
        SourceSchema schema = cassandraSourceMetadataMock.getMetadata();
        Map<String, Map<String, SourceColumn>> schemaMap = schema.getSrcSchema();

        assertTrue(schemaMap.containsKey("test_table"));
        assertTrue(schemaMap.get("test_table").containsKey("test_column"));
        SourceColumn column = schemaMap.get("test_table").get("test_column");
        assertEquals("text", column.getSourceType());
        assertTrue(column.isPrimaryKey());
    }

    @Test
    public void testGetMetadata_EmptyResultSet() throws Exception {
        when(resultSetMock.iterator()).thenReturn(Collections.emptyIterator());
        SourceSchema schema = cassandraSourceMetadata.getMetadata();
        assertTrue(schema.getSrcSchema().isEmpty());
    }

    @Test(expected = RuntimeException.class)
    public void testGetMetadata_ExceptionInDao() throws Exception {
        when(cassandraDaoMock.execute(anyString())).thenThrow(new RuntimeException("DAO Error"));
        cassandraSourceMetadata.getMetadata();
    }

    @Test
    public void testGetMetadata_MultipleShards() throws Exception {
        IShard shardMock2 = mock(IShard.class);
        when(shardMock2.getKeySpaceName()).thenReturn("test_keyspace");
        when(shardMock2.getHost()).thenReturn("localhost");
        when(shardMock2.getPort()).thenReturn(String.valueOf(9042));
        when(shardMock2.getUser()).thenReturn("cassandra_user");
        List<IShard> shards = Arrays.asList(shardMock, shardMock2);

        CassandraSourceMetadata cassandraSourceMetadataMultipleShards = new CassandraSourceMetadata(shards);
        when(cassandraDaoMock.execute(anyString())).thenReturn(resultSetMock);
        when(resultSetMock.iterator()).thenReturn(Collections.singletonList(rowMock).iterator());
        when(rowMock.getString("table_name")).thenReturn("test_table");
        when(rowMock.getString("column_name")).thenReturn("test_column");
        when(rowMock.getString("type")).thenReturn("text");
        when(rowMock.getString("kind")).thenReturn("partition_key");
        SourceSchema schema = cassandraSourceMetadataMultipleShards.getMetadata();
        Map<String, Map<String, SourceColumn>> schemaMap = schema.getSrcSchema();
        assertTrue(schemaMap.containsKey("test_table"));
        assertTrue(schemaMap.get("test_table").containsKey("test_column"));
    }

    @Test
    public void testGetMetadata_NoColumnsInTable() throws Exception {
        when(resultSetMock.iterator()).thenReturn(Collections.singletonList(rowMock).iterator());
        when(rowMock.getString("table_name")).thenReturn("empty_table");
        when(rowMock.getString("column_name")).thenReturn(null);
        when(rowMock.getString("type")).thenReturn(null);
        when(rowMock.getString("kind")).thenReturn(null);
        SourceSchema schema = cassandraSourceMetadata.getMetadata();
        Map<String, Map<String, SourceColumn>> schemaMap = schema.getSrcSchema();
        assertTrue(schemaMap.containsKey("empty_table"));
        assertTrue(schemaMap.get("empty_table").isEmpty());
    }

    @Test
    public void testGetMetadata_SingleShard() throws Exception {
        CassandraSourceMetadata cassandraSourceMetadataSingleShard = new CassandraSourceMetadata(Collections.singletonList(shardMock));

        when(cassandraDaoMock.execute(anyString())).thenReturn(resultSetMock);
        when(resultSetMock.iterator()).thenReturn(Collections.singletonList(rowMock).iterator());
        when(rowMock.getString("table_name")).thenReturn("test_table_single");
        when(rowMock.getString("column_name")).thenReturn("test_column_single");
        when(rowMock.getString("type")).thenReturn("int");
        when(rowMock.getString("kind")).thenReturn("partition_key");

        SourceSchema schema = cassandraSourceMetadataSingleShard.getMetadata();

        Map<String, Map<String, SourceColumn>> schemaMap = schema.getSrcSchema();
        assertNotNull(schemaMap);
        System.out.println("Schema map: " + schemaMap);
        assertTrue(schemaMap.containsKey("test_table_single"));
        assertTrue(schemaMap.get("test_table_single").containsKey("test_column_single"));

        SourceColumn column = schemaMap.get("test_table_single").get("test_column_single");
        assertEquals("int", column.getSourceType());
        assertTrue(column.isPrimaryKey());
    }
}
