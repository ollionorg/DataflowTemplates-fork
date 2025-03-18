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
  col10 STRING(MAX),
  col11 STRING(MAX),
  col12 STRING(MAX),
  col13 STRING(MAX),
  col14 STRING(MAX),
  col15 STRING(MAX),
  col16 STRING(MAX),
  col17 STRING(MAX),
  col18 STRING(MAX),
  col19 STRING(MAX),
  col20 STRING(MAX)
) PRIMARY KEY (id);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d'
);