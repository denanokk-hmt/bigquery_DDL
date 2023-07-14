CREATE OR REPLACE TABLE FUNCTION `flightboard_contents.tf_buttons_events_30d`(start_date DATE, end_date DATE, client_id STRING) AS (
SELECT
/*==============================================
TABLE FUNCTION::fligtboard_contents.tf_buttons_events_30d

ARGS::
* start_date : 取得開始日
* end_date : 取得終了日
* client_id : クライアントID

REMARKS
* 集計テーブル「t_buttons_events_30d」からデータを取得
==============================================*/
  e.client,
  e.extract_start_date AS date,
  e.hmt_id,
  e.buttons_id,
  e.name,
  e.text,
  e.url,
  -- URLのPathを取得
  REGEXP_SUBSTR(e.url, r'https?://[^/]+(/[^\?]*)\??.*') AS path,
  e.show_chat_button_count,
  e.click_chat_button_count
FROM
  `flightboard_contents.t_buttons_events_30d` AS e
WHERE
  e.extract_start_date >= start_date
  AND e.extract_start_date <= end_date
  AND e.client = client_id
);