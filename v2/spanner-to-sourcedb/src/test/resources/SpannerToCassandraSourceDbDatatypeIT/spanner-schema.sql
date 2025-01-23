CREATE TABLE IF NOT EXISTS alldatatypecolumns (
    varchar_column STRING(20) NOT NULL,
    tinyint_column INT64,
    text_column STRING(MAX),
    date_column DATE,
    smallint_column INT64,
    mediumint_column INT64,
    int_column INT64,
    bigint_column INT64,
    float_column FLOAT64,
    double_column FLOAT64,
    decimal_column NUMERIC,
    datetime_column TIMESTAMP,
    timestamp_column TIMESTAMP,
    time_column STRING(MAX),
    year_column STRING(MAX),
    char_column STRING(10),
    tinytext_column STRING(MAX),
    mediumtext_column STRING(MAX),
    longtext_column STRING(MAX),
    enum_column STRING(MAX),
    bool_column BOOL,
    other_bool_column BOOL,
) PRIMARY KEY(varchar_column);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d'
);