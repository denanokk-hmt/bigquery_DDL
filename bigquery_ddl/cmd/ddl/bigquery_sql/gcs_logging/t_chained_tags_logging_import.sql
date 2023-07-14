CREATE TABLE `gcs_logging.t_chained_tags_logging_import`
(
  timestamp TIMESTAMP,
  cdt TIMESTAMP,
  client_id STRING,
  customer_uuid STRING,
  hmt_id STRING,
  atid STRING,
  ddid INT64,
  gid STRING,
  gqty INT64,
  related_unixtime INT64,
  published_at STRING,
  related_words_log ARRAY<STRING>,
  g_sort INT64,
  selected_item_id STRING,
  selected_item_name STRING
)
PARTITION BY DATE(timestamp)
CLUSTER BY client_id, timestamp, hmt_id
OPTIONS(
  expiration_timestamp="2099-12-31 23:59:59 UTC",
  partition_expiration_days=400.0,
  friendly_name="chained_tag_logging",
  description="extract from GCS chained_tag_logging data.",
  labels=[("importance", "high"), ("confidentiality", "private")]
);