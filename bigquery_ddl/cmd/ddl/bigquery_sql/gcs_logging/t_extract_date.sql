BEGIN
CREATE TABLE `gcs_logging.t_extract_date`
(
  jobid STRING,
  extract_start_date STRING,
  extract_end_date STRING,
  timestamp TIMESTAMP
)
OPTIONS(
  expiration_timestamp="2099-12-31 23:59:59 UTC",
)
;

--初期値
INSERT INTO `gcs_logging.t_extract_date`(
  jobid,
  extract_start_date,
  extract_end_date,
  timestamp
)
VALUES(
  "12345",
  "2023-01-01",
  "2023-01-01",
  CURRENT_TIMESTAMP()
);

END