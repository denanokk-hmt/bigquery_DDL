CREATE OR REPLACE PROCEDURE `flightboard_contents.p_f3_1_0_get_some_content_client_ids`(
  content STRING,
  extract_start_date DATE,
  interval_days INT64,
  OUT clients ARRAY<STRING>
)
BEGIN
/*==============================================
STORED PROCEDURE::fligtboard_contents.p_f3_1_0_get_some_content_client_ids

ARGS::
  content STRING, 抽出・集計したいコンテンツ
  extract_start_date DATE, 抽出開始日
  interval_days INT64, インターバル過去日数
  OUT clients ARRAY<STRING> 取得したClientを格納して返却する

REMARKS::
・実行日の前日分のログデータに存在しているClientを取得する
==============================================*/

  --ログテーブルのもつClientを取得してArrayにして返却
  CASE

    --flight_record側から取得
    WHEN 
      content = "cust_msg"
      OR 
      content = "cust_uuid"
      OR 
      content = "op_connect_leadtime"
    THEN
      SET clients = ARRAY(SELECT client FROM `gcs_logging.tf_get_flight_record_clients`(extract_start_date, interval_days));
  
    --gyroscope側から取得
    WHEN 
      content = "events_result" 
      OR 
      content = "pageview" 
      OR 
      content = "buttons_events" 
    THEN
      SET clients = ARRAY(SELECT client FROM `gcs_logging.tf_get_gyroscope_clients`(extract_start_date, interval_days));

    --active_user_counterから取得
    WHEN content = "mau_counter"
    THEN
      SET clients = ARRAY(SELECT client FROM `gcs_logging.tf_get_active_user_counter_clients`(extract_start_date, interval_days));

    --tugcar_api_request_loggingから取得
    WHEN content = "specialtag"
    THEN
      SET clients = ARRAY(SELECT client FROM `gcs_logging.tf_get_tugcar_api_request_logging_clients`(extract_start_date, interval_days));

    --chained_tags_loggingから取得
    WHEN 
      content = "chained_tags_date_of_weekly"
      OR 
      content = "chained_tags_24h"
      OR 
      content = "chained_tags_search_word_1st"
      OR 
      content = "chained_tags_search_word_ranking"
      OR 
      content = "chained_tags_word_chain_user_ranking"
      OR 
      content = "chained_tags_word_chain_ranking"
    THEN
      SET clients = ARRAY(SELECT client FROM `gcs_logging.tf_get_chained_tags_logging_clients`(extract_start_date, interval_days));

  END CASE;

END;