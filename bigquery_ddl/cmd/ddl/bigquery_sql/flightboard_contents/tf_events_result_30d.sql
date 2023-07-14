CREATE OR REPLACE TABLE FUNCTION `flightboard_contents.tf_events_result_30d`(start_date DATE, end_date DATE, client_id STRING) AS (
SELECT
/*==============================================
TABLE FUNCTION::fligtboard_contents.tf_events_result_30d

ARGS::
* start_date : 取得開始日
* end_date : 取得終了日
* client_id : クライアントID

REMARKS
* 集計テーブル「t_events_result_30d」からデータを取得
==============================================*/
  e.client,
  e.extract_start_date AS date,
  e.label,
  e.count
FROM
  `flightboard_contents.t_events_result_30d` AS e
WHERE
  e.extract_start_date >= start_date
  AND e.extract_start_date <= end_date
  AND e.client = client_id 
);