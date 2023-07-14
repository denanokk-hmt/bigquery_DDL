CREATE OR REPLACE PROCEDURE `flightboard_contents.p_f3_4_0_sql_execute_agg_some_content`(content STRING, tmp_table STRING, client STRING, extract_start_date DATE, extract_end_date DATE, period INT64, suffix STRING)
BEGIN
/*==============================================
STORED PROCEDURE::fligtboard_contents.p_f3_4_0_sql_execute_agg_some_content

ARGS::
・content：抽出・集計したいコンテントを指定
・tmp_table：ログデータが確保されている一時テーブル名
  ※suffix="30d"の時のみ
・client：抽出・集計クライアント
・extract_start_date：抽出・集計開始日
  ※suffix="12m"の時は、=extract_month_start_date(抽出・集計月初日)
・extract_end_date：抽出・集計終了日
  ※suffix="12m"の時は、=extract_month_end_date(抽出・集計月末日)
・period：集計期間(範囲)
  ※suffix="30d"の時のみ
・suffix："30d" or "12m"

REMARKS::
・suffix="30d"→p_replace_data_agg_some_content_ndからCallされる子プロシージャであり、
・suffix="12m"→p_replace_data_agg_some_content_1mからCallされる子プロシージャであり、
　SQLを実行するプロシージャをCallする
==============================================*/

  --DELETE & INSERT INTOを実行する
  CASE content
    
    --■fligh_record系
    WHEN "cust_msg" THEN
      CALL `flightboard_contents.p_f3_4_1_sql_execute_agg_cust_msg`(tmp_table, client, extract_start_date, extract_end_date, period, suffix);
    WHEN "cust_uuid" THEN
      CALL `flightboard_contents.p_f3_4_1_sql_execute_agg_cust_uuid`(tmp_table, client, extract_start_date, extract_end_date, period, suffix);
    WHEN "op_connect_leadtime" THEN
      CALL `flightboard_contents.p_f3_4_1_sql_execute_agg_op_connect_leadtime`(tmp_table, client, extract_start_date, extract_end_date, period, suffix);
    
    --■gyroscope系
    WHEN "pageview" THEN
      CALL `flightboard_contents.p_f3_4_2_sql_execute_agg_pageview`(tmp_table, client, extract_start_date, extract_end_date, period, suffix);
    WHEN "buttons_events" THEN
      CALL `flightboard_contents.p_f3_4_2_sql_execute_agg_buttons_events`(tmp_table, client, extract_start_date, extract_end_date, period, suffix);
    WHEN "events_result" THEN
      CALL `flightboard_contents.p_f3_4_2_sql_execute_agg_events_result`(tmp_table, client, extract_start_date, extract_end_date, period, suffix);
    
    --■active_user_counter系
    WHEN "mau_counter" THEN
      CALL `flightboard_contents.p_f3_4_3_sql_execute_agg_mau_counter`(tmp_table, client, extract_start_date, extract_end_date, period, suffix);
    
    --■tugcar_api_request_logging系
    WHEN "specialtag" THEN
      CALL `flightboard_contents.p_f3_4_4_sql_execute_agg_specialtag`(tmp_table, client, extract_start_date, extract_end_date, period, suffix);
    
    --■chained_tags_logging系
    WHEN "chained_tags_date_of_weekly" THEN
      CALL `flightboard_contents.p_f3_4_5_sql_execute_agg_chained_tags_date_of_weekly`(tmp_table, client, extract_start_date, extract_end_date, period, suffix);      
    WHEN "chained_tags_24h" THEN
      CALL `flightboard_contents.p_f3_4_5_sql_execute_agg_chained_tags_24h`(tmp_table, client, extract_start_date, extract_end_date, period, suffix);
    WHEN "chained_tags_search_word_1st" THEN
      CALL `flightboard_contents.p_f3_4_5_sql_execute_agg_chained_tags_search_word_1st`(tmp_table, client, extract_start_date, extract_end_date, period, suffix);
    WHEN "chained_tags_search_word_ranking" THEN
      CALL `flightboard_contents.p_f3_4_5_sql_execute_agg_chained_tags_search_word_ranking`(tmp_table, client, extract_start_date, extract_end_date, period, suffix);
    WHEN "chained_tags_word_chain_user_ranking" THEN
      CALL `flightboard_contents.p_f3_4_5_sql_execute_agg_chained_tags_word_chain_user_ranking`(tmp_table, client, extract_start_date, extract_end_date, period, suffix);
    WHEN "chained_tags_word_chain_ranking" THEN
      CALL `flightboard_contents.p_f3_4_5_sql_execute_agg_chained_tags_word_chain_ranking`(tmp_table, client, extract_start_date, extract_end_date, period, suffix);
  END CASE;
END;