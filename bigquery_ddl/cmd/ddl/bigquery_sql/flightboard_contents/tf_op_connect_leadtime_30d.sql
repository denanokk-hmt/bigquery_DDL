CREATE OR REPLACE TABLE FUNCTION `flightboard_contents.tf_op_connect_leadtime_30d`(start_date DATE, end_date DATE, client_id STRING) AS (
SELECT
/*==============================================
TABLE FUNCTION::fligtboard_contents.tf_op_connect_leadtime_30d

ARGS::
* start_date : 取得開始日
* end_date : 取得終了日
* client_id : クライアントID

REMARKS
* 集計テーブル「t_op_connect_leadtime_30d」からデータを取得
==============================================*/
  o.client,
  o.extract_start_date AS date,
  o.min,
  o.percentile1,
  o.median,
  o.percentile90,
  o.max
FROM
  `flightboard_contents.t_op_connect_leadtime_30d` AS o
WHERE
  o.extract_start_date >= start_date
  AND o.extract_start_date <= end_date
  AND o.client = client_id
);