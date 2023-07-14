CREATE OR REPLACE TABLE FUNCTION `gcs_logging.tf_get_active_user_counter_clients`(extract_start_date DATE, interval_days INT64) AS (
SELECT DISTINCT
/*==============================================
  TABLE FUNCTION::tf_get_active_user_counter_clients

  DATASET::gcs_logging

  ARGS::
  extract_start_date DATE 抽出開始日,
  interval_days INT64: 過去に振り返る日数

  REMARKS
  t_active_user_counter_importの中からClientIDを導き出す
==============================================*/
    client
FROM `gcs_logging.t_active_user_counter_import` 
WHERE 
    client != ''
    AND DATE(timestamp, "Asia/Tokyo") >= DATE_SUB(extract_start_date, INTERVAL interval_days DAY)
ORDER BY
    client
);