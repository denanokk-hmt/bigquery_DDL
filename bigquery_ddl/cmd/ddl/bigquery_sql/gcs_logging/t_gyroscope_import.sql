CREATE TABLE `gcs_logging.t_gyroscope_import`
(
  batch_id STRING,
  cdt TIMESTAMP,
  insert_id STRING, 
  host STRING,
  url STRING,
  path STRING,
  ip STRING,
  user_agent STRING,
  referrer STRING,
  client_id STRING,
  customer_uuid STRING,
  customer_id STRING,
  hmt_id STRING,
  category_id STRING,
  type STRING,
  id STRING,
  label STRING,
  value STRING,
  wy_event STRING,
  timestamp TIMESTAMP,
  migration_id STRING
)
PARTITION BY DATE(timestamp)
OPTIONS(
  expiration_timestamp="2099-12-31 23:59:59 UTC",
  partition_expiration_days=400.0,
  friendly_name="`t_gyroscope_import`",
  description="extract from gyroscope logging data.",
  labels=[("importance", "high"), ("confidentiality", "private")]
);
