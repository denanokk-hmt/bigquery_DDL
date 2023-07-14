CREATE OR REPLACE TABLE FUNCTION `flightboard_contents.tf_buttons_page_reached_30d`(start_date DATE, end_date DATE, client_id STRING) AS (
WITH pageview AS (
/*==============================================
TABLE FUNCTION::fligtboard_contents.tf_buttons_page_reached_30d

ARGS::
* start_date : 取得開始日
* end_date : 取得終了日
* client_id : クライアントID

REMARKS
* 集計テーブル「t_buttons_events_30d」と「「t_pageviews_30d」」からデータを取得
==============================================*/
  SELECT
    extract_start_date AS date,
    -- SPLIT("?")[OFFSET(0)]: URLのGETパラメータを排除
    -- SPLIT("#")[OFFSET(0)]: URLのフラグメントを排除
    -- REGEXP_REPLACE関数：URLの最後に"/"があれば、”/”を排除
    REGEXP_REPLACE(SPLIT(SPLIT(url, "?")[OFFSET(0)], "#")[OFFSET(0)], '/$', '') AS url,
    hmt_id,
    count,
  FROM 
    `flightboard_contents.t_pageview_30d`
  WHERE
    1=1
    AND extract_start_date >= start_date
    AND extract_start_date <= end_date
    AND client = client_id
),
buttons AS (
  SELECT DISTINCT
    hmt_id,
    buttons_id,
    name,
    text,
  FROM
    `flightboard_contents.t_buttons_events_30d`
  WHERE
    extract_start_date >= start_date
    AND extract_start_date <= end_date
    AND client = client_id
    AND click_chat_button_count > 0
)
SELECT
  p.date,
  p.url,
  -- URLのPathを取得
  REGEXP_SUBSTR(p.url, r'https?://[^/]+(/[^\?]*)\??.*') AS path,
  p.hmt_id,
  b.buttons_id,
  b.name,
  b.text,
  p.count,
FROM
  pageview AS p
INNER JOIN
  buttons AS b
ON
  p.hmt_id = b.hmt_id
);