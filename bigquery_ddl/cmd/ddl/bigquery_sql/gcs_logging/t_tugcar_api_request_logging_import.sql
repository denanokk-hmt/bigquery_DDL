CREATE TABLE `gcs_logging.t_tugcar_api_request_logging_import`
(
  cdt TIMESTAMP,
  url_path STRING,
  type STRING,
  client_id STRING,
  current_params STRING,
  current_url STRING,
  query_tags STRING,
  query_item_id STRING,
  customer_uuid STRING,
  hmt_id STRING,
  search_from STRING,
  timestamp TIMESTAMP
)
PARTITION BY DATE(timestamp)
OPTIONS(
  expiration_timestamp="2099-12-31 23:59:59 UTC",
  partition_expiration_days=400.0,
  friendly_name="`tugcar_api_request_logging`",
  description="extract from tugcar_api_request_logging bucket files.",
  labels=[("importance", "high"), ("confidentiality", "private")]
);