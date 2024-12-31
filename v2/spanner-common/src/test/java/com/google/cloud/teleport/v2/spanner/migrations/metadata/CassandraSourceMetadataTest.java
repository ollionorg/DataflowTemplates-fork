package com.google.cloud.teleport.v2.spanner.migrations.metadata;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;


class CassandraSourceMetadataTest {

    @Mock
    private ResultSet mockResultSet;
    @Mock
    private Row mockRow1;
    @Mock
    private Row mockRow2;
    @Mock
    private Schema mockSchema;

    private CassandraSourceMetadata.Builder builder;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        builder = new CassandraSourceMetadata.Builder();
    }

    @Test
    void testBuilderSetSchemaAndResultSet() {
        CassandraSourceMetadata metadata = builder
                .setResultSet(mockResultSet)
                .setSchema(mockSchema)
                .build();
        Assertions.assertNotNull(metadata, "CassandraSourceMetadata should not be null");
    }

    @Test
    void testGenerateSourceSchema() {
        when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow1, mockRow2).iterator());
        when(mockRow1.getString("table_name")).thenReturn("table1");
        when(mockRow1.getString("column_name")).thenReturn("column1");
        when(mockRow1.getString("type")).thenReturn("text");
        when(mockRow1.getString("kind")).thenReturn("partition_key");

        when(mockRow2.getString("table_name")).thenReturn("table1");
        when(mockRow2.getString("column_name")).thenReturn("column2");
        when(mockRow2.getString("type")).thenReturn("int");
        when(mockRow2.getString("kind")).thenReturn("clustering");

        CassandraSourceMetadata metadata = builder
                .setResultSet(mockResultSet)
                .setSchema(mockSchema)
                .build();

        Map<String, SourceTable> sourceSchema = metadata.generateSourceSchema();
    }

    @Test
    void testGenerateAndSetSourceSchema() {
        when(mockResultSet.iterator()).thenReturn(List.of(mockRow1).iterator());
        when(mockRow1.getString("table_name")).thenReturn("table1");
        when(mockRow1.getString("column_name")).thenReturn("column1");
        when(mockRow1.getString("type")).thenReturn("text");
        when(mockRow1.getString("kind")).thenReturn("partition_key");

        CassandraSourceMetadata metadata = builder
                .setResultSet(mockResultSet)
                .setSchema(mockSchema)
                .build();

        metadata.generateAndSetSourceSchema();
    }
}
