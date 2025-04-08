CREATE TABLE singers (
  singerid INT64 NOT NULL,
  firstname STRING(MAX),
  lastname STRING(MAX),
  shardid STRING(20),
  update_ts TIMESTAMP,
) PRIMARY KEY(singerid);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d'
);