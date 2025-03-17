CREATE TABLE maxNonKeyCol (
  id STRING(MAX) NOT NULL,
  col1 STRING(MAX),
  col2 STRING(MAX),
  col3 STRING(MAX),
  col4 STRING(MAX),
  col5 STRING(MAX),
  col6 STRING(MAX),
  col7 STRING(MAX),
  col8 STRING(MAX),
  col9 STRING(MAX),
  col10 STRING(MAX)
) PRIMARY KEY (id);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d'
);