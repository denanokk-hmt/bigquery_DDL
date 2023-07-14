CREATE OR REPLACE PROCEDURE `flightboard_contents.p_f3_2_0_create_table_some_content_agg`(content STRING, suffix STRING)
BEGIN
/*==============================================
STORED PROCEDURE::fligtboard_contents.p_f3_2_0_create_table_some_content_agg

ARGS::
・content：抽出・集計したいコンテントを指定
・suffix："30d" or "12m"

REMARKS::
・flight_record or gyroscopeテーブルからlabelを集計するテーブルを作成する
・suffixが"30d”の場合、dailyデータ(日次データ)
・suffixが"12m"の場合、Mothlyデータ(月次データ)
==============================================*/
  DECLARE table_id STRING;
  DECLARE expiration_day INT64;
  /*----------------------
  ■Daily向けTABLE作成
  ----------------------*/
  IF suffix = "30d" THEN
    CASE content
    
    --flight_record

    --■■cust_msgの集計テーブル作成
    WHEN "cust_msg" THEN
      SET table_id = "flightboard_contents.t_cust_msg_30d";
      SET expiration_day = 180;
      --TABLEを作成
      EXECUTE IMMEDIATE format("""
        CREATE TABLE IF NOT EXISTS `%s`
          (
            client STRING,
            extract_start_date DATE,
            extract_end_date DATE,
            period INT64,
            current_url STRING,
            send_to STRING,
            talk_cust_type STRING,
            talk_cust_message	STRING,
            talk_cust_value	STRING,
            count INT64,
            timestamp	TIMESTAMP
          )
          PARTITION BY 
            extract_start_date
          CLUSTER BY
            client,
            extract_start_date
          OPTIONS
            (
              friendly_name = "cust_msg_day"
              , description = ""
              , partition_expiration_days=%d
              , labels = []
            )
      """, table_id, expiration_day);

    --■■cust_uuidの集計テーブル作成
    WHEN "cust_uuid" THEN
      SET table_id = "flightboard_contents.t_cust_uuid_30d";
      SET expiration_day = 180;
      --TABLEを作成
      EXECUTE IMMEDIATE format("""
        CREATE TABLE IF NOT EXISTS `%s`
          (
            client STRING,
            extract_start_date DATE,
            extract_end_date DATE,
            period INT64,
            hmt_id STRING,
            send_to STRING,
            talk_cust_type STRING,
            count INT64,
            timestamp	TIMESTAMP
          )
          PARTITION BY 
            extract_start_date
          CLUSTER BY
            client,
            extract_start_date
          OPTIONS
            (
              friendly_name = "cust_uuid_day"
              , description = ""
              , partition_expiration_days=%d
              , labels = []
            )
      """, table_id, expiration_day);

    --■■op_connect_leadtimeの集計テーブル作成
    WHEN "op_connect_leadtime" THEN
      SET table_id = "flightboard_contents.t_op_connect_leadtime_30d";
      SET expiration_day = 180;
      --TABLEを作成
      EXECUTE IMMEDIATE format("""
        CREATE TABLE IF NOT EXISTS `%s`
          (
            client STRING,
            extract_start_date DATE,
            extract_end_date DATE,
            period INT64,
            min	FLOAT64,
            percentile1	FLOAT64,
            median	FLOAT64,
            percentile90 FLOAT64,
            max	FLOAT64,
            timestamp	TIMESTAMP
          )
          PARTITION BY 
            extract_start_date
          CLUSTER BY
            client,
            extract_start_date
          OPTIONS
            (
              friendly_name = "op connect leadtime"
              , description = ""
              , partition_expiration_days=%d
              , labels = []
            )
      """, table_id, expiration_day);

    --gyroscope

    --■■pageviewの集計テーブル作成
    WHEN "pageview" THEN
      SET table_id = "flightboard_contents.t_pageview_30d";
      SET expiration_day = 180;
      --TABLEを作成
      EXECUTE IMMEDIATE format("""
        CREATE TABLE IF NOT EXISTS `%s`
          (
            client STRING,
            extract_start_date DATE,
            extract_end_date DATE,
            period INT64,
            hmt_id STRING,
            url	STRING,
            count INT64,
            timestamp	TIMESTAMP
          )
          PARTITION BY 
            extract_start_date
          CLUSTER BY
            client,
            extract_start_date
          OPTIONS
            (
              friendly_name = "pageview_d"
              , description = "pageview daily base count."
              , partition_expiration_days=%d
              , labels = []
            )
      """, table_id, expiration_day);

    --■■events_resultの集計テーブル作成
    WHEN "events_result" THEN
      SET table_id = "flightboard_contents.t_events_result_30d";
      SET expiration_day = 180;
      --TABLEを作成
      EXECUTE IMMEDIATE format("""
        CREATE TABLE IF NOT EXISTS `%s`
          (
            client STRING,
            extract_start_date DATE,
            extract_end_date DATE,
            period INT64,
            label	STRING,
            count INT64,
            timestamp	TIMESTAMP
          )
          PARTITION BY 
            extract_start_date
          CLUSTER BY
            client,
            extract_start_date
          OPTIONS
            (
              friendly_name = "events_result_d"
              , description = "events result daily base count."
              , partition_expiration_days=%d
              , labels = []
            )
      """, table_id, expiration_day);

    --■■buttons_eventsの集計テーブル作成
    WHEN "buttons_events" THEN
      SET table_id = "flightboard_contents.t_buttons_events_30d";
      SET expiration_day = 180;
      --TABLEを作成
      EXECUTE IMMEDIATE format("""
        CREATE TABLE IF NOT EXISTS `%s`
          (
            client STRING,
            extract_start_date DATE,
            extract_end_date DATE,
            period INT64,
            hmt_id STRING,
            buttons_id STRING,
            name STRING,
            text STRING,
            url STRING,
            show_chat_button_count INT64,
            click_chat_button_count INT64,
            timestamp	TIMESTAMP
          )
          PARTITION BY 
            extract_start_date
          CLUSTER BY
            client,
            extract_start_date
          OPTIONS
            (
              friendly_name = "buttons_events_d"
              , description = "events result by Buttons daily base count."
              , partition_expiration_days=%d
              , labels = []
            )
      """, table_id, expiration_day);

    --active_user_counter

    --■■mau_counterの集計テーブル作成
    WHEN "mau_counter" THEN
      SET table_id = "flightboard_contents.t_mau_counter_30d";
      SET expiration_day = 180;

      --TABLEを作成
      EXECUTE IMMEDIATE format("""
        CREATE TABLE IF NOT EXISTS `%s`
          (
            client STRING,
            extract_start_date DATE,
            extract_end_date DATE,
            period INT64,
            count INT64,
            month STRING,
            date DATE,
            timestamp	TIMESTAMP
          )
          PARTITION BY 
            extract_start_date
          CLUSTER BY
            client,
            extract_start_date
          OPTIONS
            (
              friendly_name = "active user mau"
              , description = ""
              , partition_expiration_days=%d
              , labels = []
            )
      """, table_id, expiration_day);

    --tugcar_api_request_logging

    --■■SpecialTag集計テーブル作成
    WHEN "specialtag" THEN
      SET table_id = "flightboard_contents.t_specialtag_30d";
      SET expiration_day = 180;
      --TABLEを作成
      EXECUTE IMMEDIATE format("""
        CREATE TABLE IF NOT EXISTS `%s`
          (
            client STRING,
            extract_start_date DATE,
            extract_end_date DATE,
            period INT64,
            hmt_id STRING,
            url	STRING,
            query_tags STRING,
            customer_uuid STRING,
            count INT64,
            timestamp	TIMESTAMP
          )
          PARTITION BY 
            extract_start_date
          CLUSTER BY
            client,
            extract_start_date
          OPTIONS
            (
              friendly_name = "specialtag_d"
              , description = "specialtag daily base count."
              , partition_expiration_days=%d
              , labels = []
            )
      """, table_id, expiration_day);

    --chained_tags_logging

    --■■chained_tags(月間Daily利用回数, 月間曜日別利用率)の集計テーブル作成
    WHEN "chained_tags_date_of_weekly" THEN
      SET table_id = "flightboard_contents.t_chained_tags_date_of_weekly_30d";
      SET expiration_day = 180;

      --TABLEを作成
      EXECUTE IMMEDIATE format("""
        CREATE TABLE IF NOT EXISTS `%s`
          (
            client STRING,
            extract_start_date DATE,
            extract_end_date DATE,
            period INT64,
            month STRING,
            date DATE,
            date_of_week STRING,
            count INT64,
            timestamp	TIMESTAMP
          )
          PARTITION BY 
            extract_start_date
          CLUSTER BY
            client,
            extract_start_date
          OPTIONS
            (
              friendly_name = "chained_tags_date_of_weekly"
              , description = ""
              , partition_expiration_days=%d
              , labels = []
            )
      """, table_id, expiration_day);

    --■■chained_tags(月間時間別利用数)の集計テーブル作成
    WHEN "chained_tags_24h" THEN
      SET table_id = "flightboard_contents.t_chained_tags_24h_30d";
      SET expiration_day = 180;

      --TABLEを作成
      EXECUTE IMMEDIATE format("""
        CREATE TABLE IF NOT EXISTS `%s`
          (
            client STRING,
            extract_start_date DATE,
            extract_end_date DATE,
            period INT64,
            month STRING,
            date DATE,
            hour24 INT64,
            count INT64,
            timestamp	TIMESTAMP
          )
          PARTITION BY 
            extract_start_date
          CLUSTER BY
            client,
            extract_start_date
          OPTIONS
            (
              friendly_name = "chained_tags_24h"
              , description = ""
              , partition_expiration_days=%d
              , labels = []
            )
      """, table_id, expiration_day);

    --■■chained_tags(初回検索ワード)の集計テーブル作成
    WHEN "chained_tags_search_word_1st" THEN
      SET table_id = "flightboard_contents.t_chained_tags_search_word_1st_30d";
      SET expiration_day = 180;

      --TABLEを作成
      EXECUTE IMMEDIATE format("""
        CREATE TABLE IF NOT EXISTS `%s`
          (
            client STRING,
            extract_start_date DATE,
            extract_end_date DATE,
            period INT64,
            month STRING,
            search_words STRING,
            count INT64,
            timestamp	TIMESTAMP
          )
          PARTITION BY 
            extract_start_date
          CLUSTER BY
            client,
            extract_start_date
          OPTIONS
            (
              friendly_name = "chained_tags_search_word_1st"
              , description = ""
              , partition_expiration_days=%d
              , labels = []
            )
      """, table_id, expiration_day);

    --■■chained_tags(検索ワードRanking)の集計テーブル作成
    WHEN "chained_tags_search_word_ranking" THEN
      SET table_id = "flightboard_contents.t_chained_tags_search_word_ranking_30d";
      SET expiration_day = 180;

      --TABLEを作成
      EXECUTE IMMEDIATE format("""
        CREATE TABLE IF NOT EXISTS `%s`
          (
            client STRING,
            extract_start_date DATE,
            extract_end_date DATE,
            period INT64,
            month STRING,
            ranking INT64,
            word STRING,
            count INT64,
            timestamp	TIMESTAMP
          )
          PARTITION BY 
            extract_start_date
          CLUSTER BY
            client,
            extract_start_date
          OPTIONS
            (
              friendly_name = "chained_tags_search_word_ranking"
              , description = ""
              , partition_expiration_days=%d
              , labels = []
            )
      """, table_id, expiration_day);

    --■■chained_tags(ワードチェーン利用ユーザーRanking)の集計テーブル作成
    WHEN "chained_tags_word_chain_user_ranking" THEN
      SET table_id = "flightboard_contents.t_chained_tags_word_chain_user_ranking_30d";
      SET expiration_day = 180;

      --TABLEを作成
      EXECUTE IMMEDIATE format("""
        CREATE TABLE IF NOT EXISTS `%s`
          (
            client STRING,
            extract_start_date DATE,
            extract_end_date DATE,
            period INT64,
            month STRING,
            rank INT64,
            hmt_id STRING,
            count INT64,
            timestamp	TIMESTAMP
          )
          PARTITION BY 
            extract_start_date
          CLUSTER BY
            client,
            extract_start_date
          OPTIONS
            (
              friendly_name = "chained_tags_word_chain_user_ranking"
              , description = ""
              , partition_expiration_days=%d
              , labels = []
            )
      """, table_id, expiration_day);

    --■■chained_tags(ワードチェーンRanking)の集計テーブル作成
    WHEN "chained_tags_word_chain_ranking" THEN
      SET table_id = "flightboard_contents.t_chained_tags_word_chain_ranking_30d";
      SET expiration_day = 180;

      --TABLEを作成
      EXECUTE IMMEDIATE format("""
        CREATE TABLE IF NOT EXISTS `%s`
          (
            client STRING,
            extract_start_date DATE,
            extract_end_date DATE,
            period INT64,
            month STRING,
            rank INT64,
            word_chain STRING,
            count INT64,
            hmt_id STRING,
            timestamp	TIMESTAMP
          )
          PARTITION BY 
            extract_start_date
          CLUSTER BY
            client,
            extract_start_date
          OPTIONS
            (
              friendly_name = "chained_tags_word_chain_ranking"
              , description = ""
              , partition_expiration_days=%d
              , labels = []
            )
      """, table_id, expiration_day);

    END CASE;

  /*----------------------
  ■Monthly向けTABLE作成
  ----------------------*/
  ELSEIF suffix = "12m" THEN
    CASE content
    --■■pageewの集計テーブル作成
    WHEN "pageview" THEN  
      SET table_id = "flightboard_contents.t_pageview_12m";
      SET expiration_day = 400;
    
      EXECUTE IMMEDIATE format("""
        CREATE TABLE IF NOT EXISTS `%s`
          (
            client STRING,
            extract_month STRING,
            extract_month_start_date DATE,
            hmt_id STRING,
            url	STRING,
            count INT64,
            timestamp	TIMESTAMP
          )
          PARTITION BY 
            DATE_TRUNC(extract_month_start_date, MONTH)
          CLUSTER BY
            client,
            extract_month
          OPTIONS
            (
              friendly_name = "pageview_m"
              , description = "pageview result mothly base count."
              , partition_expiration_days=%d
              , labels = []
            )
      """, table_id, expiration_day);
    --■■events_resultの集計テーブル作成
    WHEN "events_result" THEN  
      SET table_id = "flightboard_contents.t_events_result_12m";
      SET expiration_day = 400;
    
      EXECUTE IMMEDIATE format("""
        CREATE TABLE IF NOT EXISTS `%s`
          (
            client STRING,
            extract_month STRING,
            extract_month_start_date DATE,
            label	STRING,
            count INT64,
            timestamp	TIMESTAMP
          )
          PARTITION BY 
            DATE_TRUNC(extract_month_start_date, MONTH)
          CLUSTER BY
            client,
            extract_month
          OPTIONS
            (
              friendly_name = "events_result_m"
              , description = "events result mothly base count."
              , partition_expiration_days=%d
              , labels = []
            )
      """, table_id, expiration_day);
    --■■buttons_eventsの集計テーブル作成
    WHEN "buttons_events" THEN  
      SET table_id = "flightboard_contents.t_buttons_events_12m";
      SET expiration_day = 400;
    
      EXECUTE IMMEDIATE format("""
        CREATE TABLE IF NOT EXISTS `%s`
          (
            client STRING,
            extract_month STRING,
            extract_month_start_date DATE,
            buttons_id	STRING,
            name STRING,
            text STRING,
            url STRING,
            show_chat_button_count INT64,
            click_chat_button_count INT64,
            timestamp	TIMESTAMP
          )
          PARTITION BY 
            DATE_TRUNC(extract_month_start_date, MONTH)
          CLUSTER BY
            client,
            extract_month
          OPTIONS
            (
              friendly_name = "buttons_events_m"
              , description = "Buttons events mothly base count."
              , partition_expiration_days=%d
              , labels = []
            )
      """, table_id, expiration_day);
    --■■cust_msgの集計テーブル作成
    WHEN "cust_msg" THEN
      SET table_id = "flightboard_contents.t_cust_msg_12m";
      SET expiration_day = 400;
    
      EXECUTE IMMEDIATE format("""
        CREATE TABLE IF NOT EXISTS `%s`
          (
            client STRING,
            extract_month STRING,
            extract_month_start_date DATE,
            current_url STRING,
            send_to STRING,
            talk_cust_type STRING,
            talk_cust_message	STRING,
            talk_cust_value	STRING,
            count INT64,
            timestamp	TIMESTAMP
          )
          PARTITION BY 
            DATE_TRUNC(extract_month_start_date, MONTH)
          CLUSTER BY
            client,
            extract_month
          OPTIONS
            (
              friendly_name = "cust_msg_month"
              , description = ""
              , partition_expiration_days=%d
              , labels = []
            )
      """, table_id, expiration_day);
    --■■cust_uuidの集計テーブル作成
    WHEN "cust_uuid" THEN
      SET table_id = "flightboard_contents.t_cust_uuid_12m";
      SET expiration_day = 400;
    
      EXECUTE IMMEDIATE format("""
        CREATE TABLE IF NOT EXISTS `%s`
          (
            client STRING,
            extract_month STRING,
            extract_month_start_date DATE,
            send_to STRING,
            talk_cust_type STRING,
            count INT64,
            timestamp	TIMESTAMP
          )
          PARTITION BY 
            DATE_TRUNC(extract_month_start_date, MONTH)
          CLUSTER BY
            client,
            extract_month
          OPTIONS
            (
              friendly_name = "cust_uuid_month"
              , description = ""
              , partition_expiration_days=%d
              , labels = []
            )
      """, table_id, expiration_day);
    --■■op_connect_leadtimeの集計テーブル作成
    WHEN "op_connect_leadtime" THEN
      SET table_id = "flightboard_contents.t_op_connect_leadtime_12m";
      SET expiration_day = 400;
    
      EXECUTE IMMEDIATE format("""
        CREATE TABLE IF NOT EXISTS `%s`
          (
            client STRING,
            extract_month STRING,
            extract_month_start_date DATE,
            ave_min	FLOAT64,
            ave_percentile1	FLOAT64,
            ave_median	FLOAT64,
            ave_percentile90 FLOAT64,
            ave_max	FLOAT64,
            timestamp	TIMESTAMP
          )
          PARTITION BY 
            DATE_TRUNC(extract_month_start_date, MONTH)
          CLUSTER BY
            client,
            extract_month
          OPTIONS
            (
              friendly_name = "leadtime_month"
              , description = "op connect leadtime month"
              , partition_expiration_days=%d
              , labels = []
            )
      """, table_id, expiration_day);
    --■■mau_counterの集計テーブル作成
    WHEN "mau_counter" THEN
      SET table_id = "flightboard_contents.t_mau_counter_12m";
      SET expiration_day = 400;
    
      EXECUTE IMMEDIATE format("""
        CREATE TABLE IF NOT EXISTS `%s`
          (
            client STRING,
            extract_month STRING,
            extract_month_start_date DATE,
            count INT64,
            month STRING,
            timestamp	TIMESTAMP
          )
          PARTITION BY DATE_TRUNC(extract_month_start_date, MONTH)
          CLUSTER BY client, extract_month_start_date
          OPTIONS
            (
              friendly_name = "t_mau_counter_30d"
              , description = "active user counter from gcs"
              , partition_expiration_days=%d
              , labels = []
            )
      """, table_id, expiration_day);
    --■■aspecialtagの集計テーブル作成
    WHEN "specialtag" THEN
      SET table_id = "flightboard_contents.t_specialtag_12m";
      SET expiration_day = 400;
    
      EXECUTE IMMEDIATE format("""
        CREATE TABLE IF NOT EXISTS `%s`
          (
            client STRING,
            extract_month STRING,
            extract_month_start_date DATE,
            hmt_id STRING,
            url	STRING,
            query_tags STRING,
            customer_uuid STRING,
            count INT64,
            timestamp	TIMESTAMP
          )
          PARTITION BY DATE_TRUNC(extract_month_start_date, MONTH)
          CLUSTER BY client, extract_month_start_date
          OPTIONS
            (
              friendly_name = "specialtag m"
              , description = "specialtag from gcs"
              , partition_expiration_days=%d
              , labels = []
            )
      """, table_id, expiration_day);
    END CASE;
  
  END IF;
END;