CREATE OR REPLACE TABLE FUNCTION `flightboard_contents.tf_cust_msg_30d`(start_date DATE, end_date DATE, client_id STRING) AS (
SELECT
/*==============================================
TABLE FUNCTION::fligtboard_contents.tf_cust_msg_30d

ARGS::
* start_date : 取得開始日
* end_date : 取得終了日
* client_id : クライアントID

REMARKS
* 集計テーブル「t_cust_msg_30d」からデータを取得
==============================================*/
  m.client,
  m.extract_start_date AS date,
  m.current_url,
  m.send_to,
  m.talk_cust_type,
  m.talk_cust_message,
  m.talk_cust_value,
  m.count
FROM
  `flightboard_contents.t_cust_msg_30d` AS m
WHERE
  m.extract_start_date >= start_date
  AND m.extract_start_date <= end_date
  AND m.client = client_id
);