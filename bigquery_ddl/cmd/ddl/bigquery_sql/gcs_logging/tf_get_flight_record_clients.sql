CREATE OR REPLACE TABLE FUNCTION `gcs_logging.tf_get_flight_record_clients`(extract_start_date DATE, interval_days INT64) AS (
SELECT DISTINCT
/*==============================================
  TABLE FUNCTION::tf_get_flight_record_clients

  DATASET::gcs_logging

  ARGS::
  extract_start_date DATE 抽出開始日,
  interval_days INT64: 過去に振り返る日数

  REMARKS
  t_flight_record_importのの中からClientIDを導き出す
==============================================*/
    jsonPayload.common.client,
FROM `gcs_logging.t_flight_record_import` 
WHERE 
    jsonPayload.common.client != ''
    AND DATE(timestamp, "Asia/Tokyo") >= DATE_SUB(extract_start_date, INTERVAL interval_days DAY)
ORDER BY
    jsonPayload.common.client
);