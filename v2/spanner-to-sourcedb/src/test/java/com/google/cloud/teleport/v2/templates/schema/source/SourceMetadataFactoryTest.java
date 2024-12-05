package com.google.cloud.teleport.v2.templates.schema.source;

import com.google.cloud.teleport.v2.spanner.migrations.shard.IShard;
import com.google.cloud.teleport.v2.templates.metadata.source.CassandraSourceMetadata;
import com.google.cloud.teleport.v2.templates.metadata.source.ISourceMetadata;
import com.google.cloud.teleport.v2.templates.metadata.source.SourceMetadataFactory;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.List;

public class SourceMetadataFactoryTest {

    @Test
    public void testGetSourceMetadata_Cassandra() {
        IShard mockShard = mock(IShard.class);
        when(mockShard.getKeySpaceName()).thenReturn("testKeySpace");
        List<IShard> mockShards = mock(List.class);
        when(mockShards.get(0)).thenReturn(mockShard);
        when(mockShards.size()).thenReturn(1);
        String sourceType = "Cassandra";
        ISourceMetadata<SourceSchema> result = SourceMetadataFactory.getSourceMetadata(sourceType, mockShards);
        assertNotNull(result.toString(), "Expected result to be non-null");
        assertTrue("Expected result to be an instance of CassandraSourceMetadata", result instanceof CassandraSourceMetadata);
    }

    @Test
    public void testGetSourceMetadata_Mysql() {
        List<IShard> mockShards = mock(List.class);
        String sourceType = "Mysql";
        ISourceMetadata<SourceSchema> result = SourceMetadataFactory.getSourceMetadata(sourceType, mockShards);
        assertNull(result);
    }

    @Test
    public void testGetSourceMetadata_UnsupportedSourceType() {
        List<IShard> mockShards = mock(List.class);
        String sourceType = "UnsupportedSource";
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            SourceMetadataFactory.getSourceMetadata(sourceType, mockShards);
        });
        assertEquals("Unsupported sourceDbType: UnsupportedSource", exception.getMessage());
    }
}

