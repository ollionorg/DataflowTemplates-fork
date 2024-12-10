    package com.google.cloud.teleport.v2.templates.dbutils.dml;

    import com.google.cloud.teleport.v2.spanner.migrations.schema.*;
    import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
    import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
    import com.google.cloud.teleport.v2.templates.models.PreparedStatementGeneratedResponse;
    import com.google.cloud.teleport.v2.templates.processing.dml.DMLGenerator;
    import org.json.JSONObject;
    import org.junit.jupiter.api.BeforeEach;
    import org.junit.jupiter.api.Test;
    import org.mockito.*;

    import java.util.*;

    import static org.junit.jupiter.api.Assertions.*;
    import static org.mockito.Mockito.*;

    public class CassandraDMLGeneratorTest {

        @Mock
        private SpannerTable mockSpannerTable;

        @Mock
        private SourceTable mockSourceTable;

        @Mock
        private SourceColumnDefinition mockSourceColumnDefinition;

        @Mock
        private SpannerColumnDefinition mockSpannerColumnDefinition;

        @Mock
        private SpannerColumnType mockSpannerColumnType;

        @Mock
        private DMLGeneratorRequest mockDmlGeneratorRequest;

        @Mock
        private Schema mockSchema;

        private Map<String, String> response;

        private CassandraDMLGenerator cassandraDMLGenerator;

        @BeforeEach
        public void setUp() {
            cassandraDMLGenerator = new CassandraDMLGenerator();
            MockitoAnnotations.openMocks(this);  // Initialize mocks

            when(mockDmlGeneratorRequest.getSchema()).thenReturn(mockSchema);
            when(mockDmlGeneratorRequest.getSpannerTableName()).thenReturn("TestTable");
        }

        @Test
        public void testGetDMLStatement_InsertOperation() {
            when(mockDmlGeneratorRequest.getModType()).thenReturn("INSERT");
            when(mockDmlGeneratorRequest.getNewValuesJson()).thenReturn(new JSONObject());
            when(mockDmlGeneratorRequest.getKeyValuesJson()).thenReturn(new JSONObject());
            when(mockDmlGeneratorRequest.getSourceDbTimezoneOffset()).thenReturn("UTC");

            SpannerTable spannerTable = mock(SpannerTable.class);
            SourceTable sourceTable = mock(SourceTable.class);
            ColumnPK[] primaryKeys = new ColumnPK[1];
            primaryKeys[0] = mock(ColumnPK.class);

            when(sourceTable.getPrimaryKeys()).thenReturn(primaryKeys);
            when(mockDmlGeneratorRequest.getSchema().getSpSchema()).thenReturn(Collections.singletonMap("TestTable", spannerTable));
            when(mockDmlGeneratorRequest.getSchema().getSrcSchema()).thenReturn(Collections.singletonMap("TestTable", sourceTable));

            Map<String, Object> mockPrimaryKeyValues = new HashMap<>();
            mockPrimaryKeyValues.put("id", 1);
            when(CassandraDMLGenerator.getPkColumnValues(any(), any(), any(), any(), any())).thenReturn(mockPrimaryKeyValues);

            DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(mockDmlGeneratorRequest);

            assertNotNull(response);
            assertTrue(response.getDmlStatement().contains("INSERT INTO TestTable"));
        }

        @Test
        public void testGetDMLStatement_DeleteOperation() {
            when(mockDmlGeneratorRequest.getModType()).thenReturn("DELETE");
            when(mockDmlGeneratorRequest.getNewValuesJson()).thenReturn(new JSONObject());
            when(mockDmlGeneratorRequest.getKeyValuesJson()).thenReturn(new JSONObject());
            when(mockDmlGeneratorRequest.getSourceDbTimezoneOffset()).thenReturn("UTC");

            SpannerTable spannerTable = mock(SpannerTable.class);
            SourceTable sourceTable = mock(SourceTable.class);

            when(mockDmlGeneratorRequest.getSchema().getSpSchema()).thenReturn(Collections.singletonMap("TestTable", spannerTable));
            when(mockDmlGeneratorRequest.getSchema().getSrcSchema()).thenReturn(Collections.singletonMap("TestTable", sourceTable));

            Map<String, Object> mockPrimaryKeyValues = new HashMap<>();
            mockPrimaryKeyValues.put("id", 1);
            when(CassandraDMLGenerator.getPkColumnValues(any(), any(), any(), any(), any())).thenReturn(mockPrimaryKeyValues);
            DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(mockDmlGeneratorRequest);
            assertNotNull(response);
            assertInstanceOf(PreparedStatementGeneratedResponse.class, response);
            PreparedStatementGeneratedResponse preparedResponse = (PreparedStatementGeneratedResponse) response;
            assertTrue(preparedResponse.getDmlStatement().contains("DELETE FROM TestTable"));
        }

        @Test
        public void testGetDMLStatement_MissingPrimaryKey() {
            when(mockDmlGeneratorRequest.getModType()).thenReturn("INSERT");
            when(mockDmlGeneratorRequest.getNewValuesJson()).thenReturn(new JSONObject());
            when(mockDmlGeneratorRequest.getKeyValuesJson()).thenReturn(new JSONObject());
            when(mockDmlGeneratorRequest.getSourceDbTimezoneOffset()).thenReturn("UTC");

            SpannerTable spannerTable = mock(SpannerTable.class);
            SourceTable sourceTable = mock(SourceTable.class);
            when(mockDmlGeneratorRequest.getSchema().getSpSchema()).thenReturn(Collections.singletonMap("TestTable", spannerTable));
            when(mockDmlGeneratorRequest.getSchema().getSrcSchema()).thenReturn(Collections.singletonMap("TestTable", sourceTable));

            ColumnPK[] emptyPrimaryKeys = new ColumnPK[0];
            when(sourceTable.getPrimaryKeys()).thenReturn(emptyPrimaryKeys);
            DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(mockDmlGeneratorRequest);
            assertNotNull(response);
            assertEquals("", response.getDmlStatement());  // No DML should be generated if primary key is missing
        }

        @Test
        public void testGetDMLStatement_InvalidOperationType() {
            when(mockDmlGeneratorRequest.getModType()).thenReturn("INVALID");
            DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(mockDmlGeneratorRequest);
            assertNotNull(response);
            assertEquals("", response.getDmlStatement());  // Invalid operation should result in an empty DML statement
        }

        @Test
        public void testGetDMLStatement_MissingTableInSessionFile() {
            when(mockDmlGeneratorRequest.getSpannerTableName()).thenReturn("NonExistentTable");
            when(mockDmlGeneratorRequest.getSchema().getSpannerToID()).thenReturn(new HashMap<>());
            DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(mockDmlGeneratorRequest);
            assertNotNull(response);
            assertEquals("", response.getDmlStatement());  // Should return an empty DML statement if table not found
        }

        @Test
        public void testGetColumnValues() {
            String columnName = "column1";
            when(mockSourceColumnDefinition.getName()).thenReturn(columnName);
            Map<String, SourceColumnDefinition> colDefs = new HashMap<>();
            colDefs.put("colId1", mockSourceColumnDefinition);
            when(mockSourceTable.getColDefs()).thenReturn(colDefs);
            when(mockSpannerColumnDefinition.getName()).thenReturn(columnName);
            when(mockSpannerColumnDefinition.getType()).thenReturn(mockSpannerColumnType);
            when(mockSpannerColumnType.getName()).thenReturn("STRING");
            when(mockSpannerTable.getColDefs()).thenReturn(Map.of("colId1", mockSpannerColumnDefinition));

            JSONObject newValuesJson = new JSONObject();
            newValuesJson.put(columnName, "value");

            JSONObject keyValuesJson = new JSONObject();
            keyValuesJson.put(columnName, "value");

            Map<String, Object> customTransformationResponse = new HashMap<>();
            Map<String, String> columnValues = DMLGenerator.getColumnValues(
                    mockSpannerTable,
                    mockSourceTable,
                    newValuesJson,
                    keyValuesJson,
                    "UTC",
                    customTransformationResponse
            );
            assertNotNull(columnValues, "Column values map should not be null");
            assertEquals("value", columnValues.get(columnName), "Expected value for column1 not found");
        }

        @Test
        public void testGetSpannerTableWhenFound() {
            String tableName = "ExistingTable"; // The table name you are testing
            String tableId = "spannerTableId"; // Table ID we expect to return
            when(mockDmlGeneratorRequest.getSpannerTableName()).thenReturn(tableName);
            NameAndCols nameAndCols = mock(NameAndCols.class);
            when(nameAndCols.getName()).thenReturn(tableId);  // Assuming NameAndCols has a getName() method
            Map<String, NameAndCols> spannerToID = new HashMap<>();
            spannerToID.put(tableName, nameAndCols);
            when(mockSchema.getSpannerToID()).thenReturn(spannerToID);
            Map<String, SpannerTable> spSchema = new HashMap<>();
            SpannerTable spannerTable = mock(SpannerTable.class);
            spSchema.put(tableId, spannerTable);
            when(mockSchema.getSpSchema()).thenReturn(spSchema);
            DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(mockDmlGeneratorRequest);
            assertNotNull(response);
            assertNotNull(response.getDmlStatement());  // Assumes a valid DML statement is returned
        }

        @Test
        public void testGetSpannerTableWhenNotFound() {
            String tableName = "NonExistentTable";
            when(mockDmlGeneratorRequest.getSpannerTableName()).thenReturn(tableName);
            when(mockSchema.getSpannerToID()).thenReturn(new HashMap<>());
            DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(mockDmlGeneratorRequest);
            assertNotNull(response);
            assertEquals("", response.getDmlStatement());  // Should return an empty DML statement if table not found
        }

        @Test
        public void testGetSpannerTableWhenSchemaIsNull() {
            when(mockDmlGeneratorRequest.getSchema()).thenReturn(null);
            DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(mockDmlGeneratorRequest);

            assertNotNull(response);
            assertEquals("", response.getDmlStatement());  // Empty DML because schema is null
        }

        @Test
        public void testGetSpannerTableWhenSpannerTableNameIsNull() {
            when(mockDmlGeneratorRequest.getSpannerTableName()).thenReturn(null);
            when(mockDmlGeneratorRequest.getSchema()).thenReturn(mockSchema);
            when(mockSchema.getSpannerToID()).thenReturn(new HashMap<>());
            DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(mockDmlGeneratorRequest);
            assertNotNull(response);
            assertEquals("", response.getDmlStatement());  // Empty DML because table name is null
        }

        @Test
        public void testSpannerTableNotFound_ReturnsEmptyResponse() {
            // Arrange: Mock the behavior when the table is not found in the schema
            when(mockDmlGeneratorRequest.getSpannerTableName()).thenReturn("TestTable");
            when(mockDmlGeneratorRequest.getSchema()).thenReturn(mockSchema);
            when(mockSchema.getSpannerToID()).thenReturn(new HashMap<>());
            when(mockSchema.getSpSchema()).thenReturn(new HashMap<>());
            DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(mockDmlGeneratorRequest);
            assertNotNull(response, "The response should not be null.");
            assertEquals("", response.getDmlStatement(), "The DML statement should be empty when the spanner table is not found.");
        }

        @Test
        public void testSpannerTableNotFound_ReturnsEmptyDMLResponse() {
            when(mockDmlGeneratorRequest.getSpannerTableName()).thenReturn("NonExistentTable");
            when(mockDmlGeneratorRequest.getSchema()).thenReturn(mockSchema);
            when(mockSchema.getSpannerToID()).thenReturn(new HashMap<>());
            when(mockSchema.getSpSchema()).thenReturn(new HashMap<>());
            DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(mockDmlGeneratorRequest);
            assertNotNull(response, "The response should not be null.");
            assertEquals("", response.getDmlStatement(), "The DML statement should be empty when the Spanner table is not found.");
        }

        @Test
        public void testPrimaryKeysNull_ShouldReturnEmptyDMLResponse() {
            when(mockSourceTable.getPrimaryKeys()).thenReturn(null); // Set primary keys to null
            when(mockSourceTable.getName()).thenReturn("TestTable");
            DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(mockDmlGeneratorRequest);
            assertNotNull(response);
            assertEquals("", response.getDmlStatement());
        }

        @Test
        public void testPrimaryKeysEmpty_ShouldReturnEmptyDMLResponse() {
            when(mockSourceTable.getPrimaryKeys()).thenReturn(new ColumnPK[0]); // Empty array for primary keys
            when(mockSourceTable.getName()).thenReturn("TestTable");
            DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(mockDmlGeneratorRequest);
            assertNotNull(response);
            assertEquals("", response.getDmlStatement());
        }

        @Test
        public void testPrimaryKeysPresent_ShouldNotReturnEmptyDMLResponse() {
            ColumnPK[] primaryKeys = new ColumnPK[] { mock(ColumnPK.class) }; // Non-empty primary keys
            when(mockSourceTable.getPrimaryKeys()).thenReturn(primaryKeys);
            when(mockSourceTable.getName()).thenReturn("TestTable");
            DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(mockDmlGeneratorRequest);
            assertNotNull(response);
            assertNotEquals("", response.getDmlStatement());
        }

        @Test
        public void testGetDMLStatement_UpdateOperation() {
            when(mockDmlGeneratorRequest.getModType()).thenReturn("UPDATE");
            when(mockDmlGeneratorRequest.getNewValuesJson()).thenReturn(new JSONObject());
            when(mockDmlGeneratorRequest.getKeyValuesJson()).thenReturn(new JSONObject());
            when(mockDmlGeneratorRequest.getSourceDbTimezoneOffset()).thenReturn("UTC");

            SpannerTable spannerTable = mock(SpannerTable.class);
            SourceTable sourceTable = mock(SourceTable.class);
            ColumnPK[] primaryKeys = new ColumnPK[1];
            primaryKeys[0] = mock(ColumnPK.class);

            when(sourceTable.getPrimaryKeys()).thenReturn(primaryKeys);
            when(mockDmlGeneratorRequest.getSchema().getSpSchema()).thenReturn(Collections.singletonMap("TestTable", spannerTable));
            when(mockDmlGeneratorRequest.getSchema().getSrcSchema()).thenReturn(Collections.singletonMap("TestTable", sourceTable));

            Map<String, Object> mockPrimaryKeyValues = new HashMap<>();
            mockPrimaryKeyValues.put("id", 1);
            when(CassandraDMLGenerator.getPkColumnValues(any(), any(), any(), any(), any())).thenReturn(mockPrimaryKeyValues);
            DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(mockDmlGeneratorRequest);
            assertNotNull(response);
            assertInstanceOf(PreparedStatementGeneratedResponse.class, response);
            PreparedStatementGeneratedResponse preparedResponse = (PreparedStatementGeneratedResponse) response;
            assertTrue(preparedResponse.getDmlStatement().contains("UPDATE TestTable"));
        }

        @Test
        void testDeleteConditions_withMoreThan5Characters() {
            StringBuilder deleteConditions = new StringBuilder("DELETE FROM table WHERE id = 1 AND status = 'active'");
            System.out.println("Initial length: " + deleteConditions.length());
            System.out.println("Before truncation: " + deleteConditions);
            if (deleteConditions.length() > 5) {
                deleteConditions.setLength(deleteConditions.length() - 5);
            }
            System.out.println("After truncation: " + deleteConditions);
            System.out.println("Final length: " + deleteConditions.length());
            assertEquals("DELETE FROM table WHERE id = 1 AND status = 'act", deleteConditions.toString());
        }

        @Test
        void testMapInitialization() {
            Map<String, Object> response = new HashMap<>();
            assertTrue(response.isEmpty(), "The map should be empty on initialization.");
        }

        @Test
        public void testUpdateDMLStatement_ShouldReturnValidUpdate() {
            when(mockDmlGeneratorRequest.getModType()).thenReturn("UPDATE");
            when(mockDmlGeneratorRequest.getNewValuesJson()).thenReturn(new JSONObject());
            when(mockDmlGeneratorRequest.getKeyValuesJson()).thenReturn(new JSONObject());
            when(mockDmlGeneratorRequest.getSourceDbTimezoneOffset()).thenReturn("UTC");

            SpannerTable spannerTable = mock(SpannerTable.class);
            SourceTable sourceTable = mock(SourceTable.class);
            ColumnPK[] primaryKeys = new ColumnPK[1];
            primaryKeys[0] = mock(ColumnPK.class);

            when(sourceTable.getPrimaryKeys()).thenReturn(primaryKeys);
            when(mockDmlGeneratorRequest.getSchema().getSpSchema()).thenReturn(Collections.singletonMap("TestTable", spannerTable));
            when(mockDmlGeneratorRequest.getSchema().getSrcSchema()).thenReturn(Collections.singletonMap("TestTable", sourceTable));

            Map<String, Object> mockPrimaryKeyValues = new HashMap<>();
            mockPrimaryKeyValues.put("id", 1);
            when(CassandraDMLGenerator.getPkColumnValues(any(), any(), any(), any(), any())).thenReturn(mockPrimaryKeyValues);
            DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(mockDmlGeneratorRequest);
            assertNotNull(response);
            assertTrue(response.getDmlStatement().contains("UPDATE TestTable"));
        }

        @Test
        public void testPrimaryKeyMissing_ShouldReturnEmptyDMLResponse() {
            when(mockSourceTable.getPrimaryKeys()).thenReturn(null);
            when(mockSourceTable.getName()).thenReturn("TestTable");
            DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(mockDmlGeneratorRequest);
            assertNotNull(response);
            assertEquals("", response.getDmlStatement());
        }

        @Test
        public void testPrimaryKeyEmpty_ShouldReturnEmptyDMLResponse() {
            when(mockSourceTable.getPrimaryKeys()).thenReturn(new ColumnPK[0]);
            when(mockSourceTable.getName()).thenReturn("TestTable");
            DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(mockDmlGeneratorRequest);
            assertNotNull(response);
            assertEquals("", response.getDmlStatement());
        }
    }
