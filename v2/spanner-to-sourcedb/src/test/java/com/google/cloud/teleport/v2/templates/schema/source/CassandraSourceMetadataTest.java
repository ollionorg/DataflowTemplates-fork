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

        // Setup mock shard
        when(shardMock.getKeySpaceName()).thenReturn("test_keyspace");
        when(shardMock.getHost()).thenReturn("localhost");
        when(shardMock.getPort()).thenReturn(String.valueOf(9042));
        when(shardMock.getUser()).thenReturn("cassandra_user");

        // Initialize CassandraSourceMetadata
        List<IShard> shards = Collections.singletonList(shardMock);
        cassandraSourceMetadata = new CassandraSourceMetadata(shards);

        // Mock the DAO creation
        String connectionKey = "localhost:9042/cassandra_user/test_keyspace";
        when(cassandraDaoMock.read(anyString())).thenReturn(resultSetMock);
    }

    @Test
    public void testGetMetadata_Success() throws Exception {
        // Arrange mock ResultSet
        when(resultSetMock.iterator()).thenReturn(Collections.singletonList(rowMock).iterator());
        when(rowMock.getString("table_name")).thenReturn("test_table");
        when(rowMock.getString("column_name")).thenReturn("test_column");
        when(rowMock.getString("type")).thenReturn("text");
        when(rowMock.getString("kind")).thenReturn("partition_key");

        // Act
        SourceSchema schema = cassandraSourceMetadata.getMetadata();

        // Assert the metadata is as expected
        Map<String, Map<String, SourceColumn>> schemaMap = schema.getSrcSchema();
        assertTrue(schemaMap.containsKey("test_table"));
        assertTrue(schemaMap.get("test_table").containsKey("test_column"));

        SourceColumn column = schemaMap.get("test_table").get("test_column");
        assertEquals("text", column.getSourceType());
        assertTrue(column.isPrimaryKey());
    }

    @Test
    public void testGetMetadata_EmptyResultSet() throws Exception {
        // Arrange: empty result set
        when(resultSetMock.iterator()).thenReturn(Collections.emptyIterator());

        // Act
        SourceSchema schema = cassandraSourceMetadata.getMetadata();

        // Assert: schema should be empty
        assertTrue(schema.getSrcSchema().isEmpty());
    }

    @Test(expected = RuntimeException.class)
    public void testGetMetadata_ExceptionInDao() throws Exception {
        // Arrange: mock DAO to throw an exception
        when(cassandraDaoMock.read(anyString())).thenThrow(new RuntimeException("DAO Error"));

        // Act & Assert: calling getMetadata should throw RuntimeException
        cassandraSourceMetadata.getMetadata();
    }

    @Test
    public void testGetMetadata_MultipleShards() throws Exception {
        // Arrange: Set up multiple shards
        IShard shardMock2 = mock(IShard.class);
        when(shardMock2.getKeySpaceName()).thenReturn("test_keyspace");
        when(shardMock2.getHost()).thenReturn("localhost");
        when(shardMock2.getPort()).thenReturn(String.valueOf(9042));
        when(shardMock2.getUser()).thenReturn("cassandra_user");
        List<IShard> shards = Arrays.asList(shardMock, shardMock2);

        CassandraSourceMetadata cassandraSourceMetadataMultipleShards = new CassandraSourceMetadata(shards);

        // Mock the DAO for multiple shards
        when(cassandraDaoMock.read(anyString())).thenReturn(resultSetMock);
        when(resultSetMock.iterator()).thenReturn(Collections.singletonList(rowMock).iterator());
        when(rowMock.getString("table_name")).thenReturn("test_table");
        when(rowMock.getString("column_name")).thenReturn("test_column");
        when(rowMock.getString("type")).thenReturn("text");
        when(rowMock.getString("kind")).thenReturn("partition_key");

        // Act
        SourceSchema schema = cassandraSourceMetadataMultipleShards.getMetadata();

        // Assert the metadata for multiple shards
        Map<String, Map<String, SourceColumn>> schemaMap = schema.getSrcSchema();
        assertTrue(schemaMap.containsKey("test_table"));
        assertTrue(schemaMap.get("test_table").containsKey("test_column"));
    }

    @Test
    public void testGetMetadata_NoColumnsInTable() throws Exception {
        // Arrange: No columns in the result set for the table
        when(resultSetMock.iterator()).thenReturn(Collections.singletonList(rowMock).iterator());
        when(rowMock.getString("table_name")).thenReturn("empty_table");
        when(rowMock.getString("column_name")).thenReturn(null);
        when(rowMock.getString("type")).thenReturn(null);
        when(rowMock.getString("kind")).thenReturn(null);

        // Act
        SourceSchema schema = cassandraSourceMetadata.getMetadata();

        // Assert: The table exists but has no columns
        Map<String, Map<String, SourceColumn>> schemaMap = schema.getSrcSchema();
        assertTrue(schemaMap.containsKey("empty_table"));
        assertTrue(schemaMap.get("empty_table").isEmpty());
    }

    @Test
    public void testGetMetadata_SingleShard() throws Exception {
        // Test case for a single shard to ensure behavior is the same with fewer shards
        CassandraSourceMetadata cassandraSourceMetadataSingleShard = new CassandraSourceMetadata(Collections.singletonList(shardMock));

        when(cassandraDaoMock.read(anyString())).thenReturn(resultSetMock);
        when(resultSetMock.iterator()).thenReturn(Collections.singletonList(rowMock).iterator());
        when(rowMock.getString("table_name")).thenReturn("test_table_single");
        when(rowMock.getString("column_name")).thenReturn("test_column_single");
        when(rowMock.getString("type")).thenReturn("int");
        when(rowMock.getString("kind")).thenReturn("partition_key");

        SourceSchema schema = cassandraSourceMetadataSingleShard.getMetadata();

        Map<String, Map<String, SourceColumn>> schemaMap = schema.getSrcSchema();
        assertTrue(schemaMap.containsKey("test_table_single"));
        assertTrue(schemaMap.get("test_table_single").containsKey("test_column_single"));

        SourceColumn column = schemaMap.get("test_table_single").get("test_column_single");
        assertEquals("int", column.getSourceType());
        assertTrue(column.isPrimaryKey());
    }
}
