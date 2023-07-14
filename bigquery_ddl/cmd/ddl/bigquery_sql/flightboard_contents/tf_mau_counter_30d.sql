CREATE OR REPLACE TABLE FUNCTION `flightboard_contents.tf_mau_counter_30d`(
  start_date DATE, 
  end_date DATE, 
  client_id STRING
) AS (
SELECT
/*==============================================
TABLE FUNCTION::fligtboard_contents.tf_mau_counter_30d

ARGS::
* start_date : 取得開始日
* end_date : 取得終了日
* client_id : クライアントID

REMARKS
* 集計テーブル「t_mau_counter_30d」からデータを取得
==============================================*/
  m.client,
  max(m.count) AS count,
  m.month
FROM
  `flightboard_contents.t_mau_counter_30d` AS m
WHERE
  m.extract_start_date >= start_date
  AND m.extract_start_date <= end_date
  AND m.client = client_id
GROUP BY
  m.client,
  m.month
);