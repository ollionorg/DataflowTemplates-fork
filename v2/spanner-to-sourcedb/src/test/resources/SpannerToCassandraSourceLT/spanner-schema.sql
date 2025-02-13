CREATE TABLE `person` (
    `id` STRING(100) NOT NULL,
    `first_name` STRING(100)
) PRIMARY KEY (`id`);


CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d'
);