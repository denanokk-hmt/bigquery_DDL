CREATE TABLE `flightboard.t_result_logging_extract`
(
  timestamp TIMESTAMP,
  kind STRING,
  job_id STRING,
  job_name STRING,
  frequency STRING,
  client STRING,
  extract_start_date DATE,
  extract_end_date DATE,
  extract_qty INT64
)
PARTITION BY DATE(timestamp)
OPTIONS(
  partition_expiration_days=400.0,
  friendly_name="results",
  description="batch results."
);