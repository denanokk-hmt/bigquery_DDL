CREATE TABLE `gcs_logging.t_flight_record_import`
(
  Cdt TIMESTAMP,
  jsonPayload STRUCT<common STRUCT<hmt_id STRING, type STRING, logid STRING, api STRING, client STRING, service STRING, component STRING, customer_uuid STRING, use STRING, session_id STRING>, response STRUCT<body STRING, headers STRING, responded STRING>, request STRUCT<body STRING, headers STRING>, session STRUCT<token STRING, uid STRING, rid STRING, op_session STRING, op_rid STRING, op_access_token STRING, op_cust_uid STRING, op_system STRING, op_ope_uid STRING>, kind STRING, customer_uuid STRING, logid STRING>,
  timestamp TIMESTAMP
)
PARTITION BY DATE(timestamp)
OPTIONS(
  expiration_timestamp="2099-12-31 23:59:59 UTC",
  partition_expiration_days=400.0,
  friendly_name="`t_flight_record_import`",
  description="extract from flight_recorder logging data.",
  labels=[("importance", "high"), ("confidentiality", "private")]
);