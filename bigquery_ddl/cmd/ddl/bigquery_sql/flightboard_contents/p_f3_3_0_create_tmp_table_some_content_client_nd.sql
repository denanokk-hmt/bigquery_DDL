CREATE OR REPLACE PROCEDURE `flightboard_contents.p_f3_3_0_create_tmp_table_some_content_client_nd`(
  content STRING, 
  client STRING, 
  extract_start_date DATE, 
  extract_end_date DATE, 
  OUT tmp_table STRING)
BEGIN
/*==============================================
STORED PROCEDURE::fligtboard_contents.p_f3_3_0_create_tmp_table_some_content_client_nd

ARGS::
・content：抽出・集計したいコンテントを指定
・client：抽出・集計クライアント
・extract_start_date：抽出・集計開始日
・extract_end_date：抽出・集計終了日
・tmp_table：一時テーブル名を返却する

REMARKS::
・集計元のデータ(clinet毎)から指定期間を抽出し、一時テーブルに確保
==============================================*/

  DECLARE extract_apis ARRAY<STRING>;

  CASE
    ----------------------------------------------------------
    --■■ cust_msg, cust_uuidの集計テーブル作成
    WHEN 
      content = "cust_msg" 
      OR 
      content = "cust_uuid" 
      THEN
        --メッセージ条件を設定
        SET extract_apis = ["POST_MESSAGE"];
        --flight_record_365d_[client]から指定期間内のレコードを抽出し、JSONパースして、TEMPテーブルへ収納する(TmpTbl_FlightRecord_[client])
        CALL `flightboard_contents.p_f3_3_1_create_tmp_table_flight_record_365d_client_nd_json_parse`(client, extract_start_date, extract_end_date, extract_apis, tmp_table);

    ----------------------------------------------------------
    --■■ op_connect_leadtimeの集計テーブル作成
    WHEN 
      content = "op_connect_leadtime" 
      THEN
        --bot, op用のメッセージ条件を設定
        SET extract_apis = ["POST_MESSAGE","POST_OP_RECEIVE_MESSAGE"];

        --①::flight_record_365d_[client]から指定期間内のレコードを抽出し、JSONパースして、TEMPテーブルへ収納する
        --(TmpTbl_FlightRecord_[client]_[yyyymmdd]_[yyyymmdd])
        CALL `flightboard_contents.p_f3_3_1_create_tmp_table_flight_record_365d_client_nd_json_parse`(client, extract_start_date, extract_end_date, extract_apis, tmp_table);

        --②::①から、オペ接続リードタイムに利用するBot側のレコードを、TEMPテーブルへ格納する(①_bot)
        --(TmpTbl_FlightRecord_[client]_[yyyymmdd]_[yyyymmdd]_bot)
        CALL `flightboard_contents.p_f3_3_1_get_data_op_connect_leadtime_msgs`(tmp_table, "bot");

        --③::①から、オペ接続リードタイムに利用するOpe側のレコードを、TEMPテーブル③へ格納する(①_op)
        --(TmpTbl_FlightRecord_[client]_[yyyymmdd]_[yyyymmdd]_op)
        CALL `flightboard_contents.p_f3_3_1_get_data_op_connect_leadtime_msgs`(tmp_table, "op");

    ----------------------------------------------------------
    --■■ events_result, pageview, buttons_eventsの集計テーブル作成
    WHEN 
      content = "events_result"  
      OR 
      content = "pageview" 
      OR 
      content = "buttons_events" 
      THEN
        --gyroscope_365d_[client]から指定期間内のレコードを抽出し、TEMPテーブルへ収納する(TmpTbl_Gyroscope_[client])
        CALL `flightboard_contents.p_f3_3_2_create_tmp_table_gyroscope_client_nd`(content, client, extract_start_date, extract_end_date, tmp_table);

    ----------------------------------------------------------
    --■■ mau_counの集計テーブル作成
    WHEN
      content = "mau_counter" 
      THEN
        --t_mau_counterから指定期間内のレコードを抽出し、TEMPテーブルへ収納する
        CALL `flightboard_contents.p_f3_3_3_create_tmp_table_mau_counter_client_nd`(client, extract_start_date, extract_end_date, tmp_table);

    ----------------------------------------------------------
    --■■ specialtagの集計テーブル作成
    WHEN 
      content = "specialtag" 
      THEN
        -- tugcar_api_request_loggingから指定期間内のレコードを抽出し、TEMPテーブルへ収納する
        CALL `flightboard_contents.p_f3_3_4_create_tmp_table_tugcar_api_request_logging_client_nd`(client, extract_start_date, extract_end_date, tmp_table);

    ----------------------------------------------------------
    --■■ chained_tagsの集計テーブル作成
    WHEN 
      content = "chained_tags"
      OR
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
        -- chained_tags_loggingから指定期間内のレコードを抽出し、TEMPテーブルへ収納する
        CALL `flightboard_contents.p_f3_3_5_create_tmp_table_chained_tags_logging_client_nd`(client, extract_start_date, extract_end_date, tmp_table);

  END CASE;
END;