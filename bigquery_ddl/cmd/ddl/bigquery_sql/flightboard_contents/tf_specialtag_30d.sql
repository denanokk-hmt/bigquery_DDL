CREATE OR REPLACE TABLE FUNCTION `flightboard_contents.tf_specialtag_30d`(start_date DATE, end_date DATE, client_id STRING) AS (
SELECT 
/*==============================================
TABLE FUNCTION::flightboard_contents.tf_specialtag_30d

ARGS::
* start_date : 取得開始日
* end_date : 取得終了日
* client_id : クライアントID

REMARKS
* 集計テーブル「t_pageview_30d」からデータを取得
==============================================*/
  i.client,
  i.hmt_id,
  i.url,
  i.query_tags,
  i.customer_uuid,
  i.extract_start_date AS date,
  i.count 
FROM
  `flightboard_contents.t_specialtag_30d` AS i
WHERE 
  i.extract_start_date >= start_date
  AND i.extract_end_date <= end_date
  AND i.client = client_id
);