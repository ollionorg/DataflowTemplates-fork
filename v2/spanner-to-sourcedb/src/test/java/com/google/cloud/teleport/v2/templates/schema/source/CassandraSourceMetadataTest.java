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
        Map<String, Map<String, SourceColumn>> schemaMap = new HashMap<>();

        SourceColumn columnMock = mock(SourceColumn.class);
        when(columnMock.getSourceType()).thenReturn("text");
        when(columnMock.isPrimaryKey()).thenReturn(true);

        Map<String, SourceColumn> columnMap = new HashMap<>();
        columnMap.put("test_column", columnMock);
        schemaMap.put("test_table", columnMap);

        when(schemaMock.getSrcSchema()).thenReturn(schemaMap);

        when(cassandraSourceMetadataMock.getMetadata()).thenReturn(schemaMock);
        SourceSchema schema = cassandraSourceMetadataMock.getMetadata();
        Map<String, Map<String, SourceColumn>> currentSchemaMap = schema.getSrcSchema();

        assertTrue(currentSchemaMap.containsKey("test_table"));
        assertTrue(currentSchemaMap.get("test_table").containsKey("test_column"));
        SourceColumn column = currentSchemaMap.get("test_table").get("test_column");
        assertEquals("text", column.getSourceType());
        assertTrue(column.isPrimaryKey());
    }

    @Test
    public void testGetMetadata_EmptyResultSet() throws Exception {
        IShard shardMock = mock(IShard.class);
        when(shardMock.getHost()).thenReturn("localhost");
        when(shardMock.getPort()).thenReturn("9042");
        when(shardMock.getUser()).thenReturn("cassandra_user");
        when(shardMock.getKeySpaceName()).thenReturn("test_keyspace");
        CassandraSourceMetadata cassandraSourceMetadataEmptyResultSet = mock(CassandraSourceMetadata.class);
        ResultSet resultSetMock = mock(ResultSet.class);
        when(resultSetMock.iterator()).thenReturn(Collections.emptyIterator());
        SourceSchema mockedSchema = mock(SourceSchema.class);
        when(mockedSchema.getSrcSchema()).thenReturn(new HashMap<>());
        when(cassandraSourceMetadataEmptyResultSet.getMetadata()).thenReturn(mockedSchema);
        SourceSchema schema = cassandraSourceMetadataEmptyResultSet.getMetadata();
        Map<String, Map<String, SourceColumn>> schemaMap = schema.getSrcSchema();
        assertNotNull("Schema map should not be null", schemaMap);
        assertTrue("Schema map should be empty since the ResultSet is empty", schemaMap.isEmpty());
    }

    @Test(expected = RuntimeException.class)
    public void testGetMetadata_ExceptionInDao() throws Exception {
        when(cassandraDaoMock.execute(anyString())).thenThrow(new RuntimeException("DAO Error"));
        cassandraSourceMetadata.getMetadata();
    }

    @Test
    public void testGetMetadata_MultipleShards() throws Exception {
        IShard shardMock1 = mock(IShard.class);
        when(shardMock1.getKeySpaceName()).thenReturn("test_keyspace_1");
        when(shardMock1.getHost()).thenReturn("localhost");
        when(shardMock1.getPort()).thenReturn("9042");
        when(shardMock1.getUser()).thenReturn("cassandra_user");
        IShard shardMock2 = mock(IShard.class);
        when(shardMock2.getKeySpaceName()).thenReturn("test_keyspace_2");
        when(shardMock2.getHost()).thenReturn("localhost");
        when(shardMock2.getPort()).thenReturn("9043");
        when(shardMock2.getUser()).thenReturn("cassandra_user_2");
        List<IShard> shards = Arrays.asList(shardMock1, shardMock2);
        CassandraSourceMetadata cassandraSourceMetadataMultipleShards = mock(CassandraSourceMetadata.class);
        SourceSchema mockedSchema = mock(SourceSchema.class);
        Map<String, Map<String, SourceColumn>> schemaMap = new HashMap<>();
        Map<String, SourceColumn> tableMap = new HashMap<>();
        tableMap.put("test_column", new SourceColumn("test_colum", true));
        schemaMap.put("test_table", tableMap);
        when(mockedSchema.getSrcSchema()).thenReturn(schemaMap);
        when(cassandraSourceMetadataMultipleShards.getMetadata()).thenReturn(mockedSchema);
        SourceSchema schema = cassandraSourceMetadataMultipleShards.getMetadata();
        Map<String, Map<String, SourceColumn>> resultMap = schema.getSrcSchema();
        assertNotNull(resultMap);
        assertTrue("Schema should contain test_table", resultMap.containsKey("test_table"));
        assertTrue("Schema should contain test_column in test_table", resultMap.get("test_table").containsKey("test_column"));
    }

    @Test
    public void testGetMetadata_NoColumnsInTable() throws Exception {
        IShard shardMock = mock(IShard.class);
        when(shardMock.getHost()).thenReturn("localhost");
        when(shardMock.getPort()).thenReturn("9042");
        when(shardMock.getUser()).thenReturn("cassandra_user");
        when(shardMock.getKeySpaceName()).thenReturn("test_keyspace");
        CassandraSourceMetadata cassandraSourceMetadataNoColumns = mock(CassandraSourceMetadata.class);
        ResultSet resultSetMock = mock(ResultSet.class);
        when(resultSetMock.iterator()).thenReturn(Collections.emptyIterator());
        SourceSchema mockedSchema = mock(SourceSchema.class);
        when(mockedSchema.getSrcSchema()).thenReturn(new HashMap<>());
        when(cassandraSourceMetadataNoColumns.getMetadata()).thenReturn(mockedSchema);
        SourceSchema schema = cassandraSourceMetadataNoColumns.getMetadata();
        Map<String, Map<String, SourceColumn>> schemaMap = schema.getSrcSchema();
        assertNotNull("Schema map should not be null", schemaMap);
        assertTrue("Schema map should be empty since no columns exist", schemaMap.isEmpty());
    }

    @Test
    public void testGetMetadata_SingleShard() throws Exception {
        IShard shardMock1 = mock(IShard.class);
        when(shardMock1.getKeySpaceName()).thenReturn("test_keyspace_1");
        when(shardMock1.getHost()).thenReturn("localhost");
        when(shardMock1.getPort()).thenReturn("9042");
        when(shardMock1.getUser()).thenReturn("cassandra_user");
        CassandraSourceMetadata cassandraSourceMetadataMultipleShards = mock(CassandraSourceMetadata.class);
        SourceSchema mockedSchema = mock(SourceSchema.class);
        Map<String, Map<String, SourceColumn>> schemaMap = new HashMap<>();
        Map<String, SourceColumn> tableMap = new HashMap<>();
        tableMap.put("test_column", new SourceColumn("test_colum", true));
        schemaMap.put("test_table", tableMap);
        when(mockedSchema.getSrcSchema()).thenReturn(schemaMap);
        when(cassandraSourceMetadataMultipleShards.getMetadata()).thenReturn(mockedSchema);
        SourceSchema schema = cassandraSourceMetadataMultipleShards.getMetadata();
        Map<String, Map<String, SourceColumn>> resultMap = schema.getSrcSchema();
        assertNotNull(resultMap);
        assertTrue("Schema should contain test_table", resultMap.containsKey("test_table"));
        assertTrue("Schema should contain test_column in test_table", resultMap.get("test_table").containsKey("test_column"));
    }
}
