CREATE TABLE `gcs_logging.t_active_user_counter_import`
(
  client STRING,
  count INT64,
  cdt TIMESTAMP,
  month STRING,
  type STRING,
  timestamp TIMESTAMP,
)
PARTITION BY DATE(timestamp)
CLUSTER BY client, type, month
OPTIONS(
  expiration_timestamp="2099-12-31 23:59:59 UTC",
  partition_expiration_days=400.0,
  friendly_name="active_user_counter",
  description="active user counter from gcs"
);