CREATE OR REPLACE TABLE FUNCTION `flightboard_contents.tf_specialtag_page_reached_30d`(start_date DATE, end_date DATE, client STRING) AS (
WITH pageview AS (
/*==============================================
TABLE FUNCTION::flightboard_contents.tf_specialtag_page_reached

ARGS::
* start_date : 取得開始日
* end_date : 取得終了日
* client_id : クライアントID

REMARKS
* flightboard_contents.2用
==============================================*/
  SELECT
      p.client,
      p.extract_start_date AS date,
      p.hmt_id,
      p.url,
      -- URLのPathを取得
      REGEXP_SUBSTR(p.url, r'https?://[^/]+(/[^\?]*)\??.*') AS path,
      COUNT(*) AS count
    FROM
      `flightboard_contents.t_pageview_30d` AS p
    WHERE
      p.extract_start_date >= start_date
      AND p.extract_start_date <= end_date
      AND p.client = client
    GROUP BY
      p.client,
      p.extract_start_date,
      p.hmt_id,
      p.url
), 
logging AS (
  SELECT DISTINCT
    l.hmt_id,
    l.query_tags,
  FROM
    `gcs_logging.t_tugcar_api_request_logging_import` AS l
  WHERE
    DATE(l.timestamp, "Asia/Tokyo") >= start_date
    AND DATE(l.timestamp, "Asia/Tokyo") <= end_date
    AND l.client_id = client
    AND l.url_path = "/hmt/attachment/search/SpecialTag"
    AND l.hmt_id IS NOT NULL
    AND l.query_tags IS NOT NULL
)
SELECT
  p.date,
  l.query_tags,
  p.url,
  p.path,
  SUM(p.count) AS count,
FROM
  pageview AS p
INNER JOIN
  logging AS l
ON
  p.hmt_id = l.hmt_id
GROUP BY
  p.date,
  l.query_tags,
  p.url,
  p.path
);