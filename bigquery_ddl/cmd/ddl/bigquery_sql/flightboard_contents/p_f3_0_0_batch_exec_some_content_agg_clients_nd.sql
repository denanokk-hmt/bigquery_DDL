CREATE OR REPLACE PROCEDURE `flightboard_contents.p_f3_0_0_batch_exec_some_content_agg_clients_nd`(
  job_id STRING, 
  job_name STRING,
  content STRING, 
  extract_start_date STRING, 
  extract_end_date STRING, 
  period INT64
)
BEGIN
/*==============================================
STORED PROCEDURE::flightboard_contents.p_f3_0_0_batch_exec_some_content_agg_clients_nd

[ARGS]
job_id STRING::GCP Workflowのjob id, 
job_name STRING::GCP Workflowのjob name, 
content STRING::抽出・集計したいコンテントを指定,
extract_start_date STRING::集計期間開始日(Default:実行日の前日)
period INT64::集計期間終了日(Default:実行日の前日)

REMARKS::
・contentで予定されているもの（拡張あり）
  events_result
  op_connect_leadtime
  cust_msg
・p_events_result_agg_ndプロシージャーを全Client数分実行させる親プロシージャ
・全CliendIDは、ログのレコードから取得する。
　（バッチ実行用）
==============================================*/

  DECLARE theDayBefore STRING;
  DECLARE interval_days INT64;
  DECLARE clients ARRAY<STRING>;
  DECLARE client STRING;
  DECLARE destiny_dataset_table STRING;

  --実行する前日を取得
  SET theDayBefore = CAST((
    SELECT CURRENT_DATE("Asia/Tokyo") - 1
  ) AS STRING);

  --抽出日を決定(抽出開始日に日付指定がない[=Dailyバッチ運用]場合、前日を設定する)
  --集計期間を決定(抽出開始日に日付指定がない[=Dailyバッチ運用]場合、1を設定する)
  CASE 
    WHEN extract_start_date IS NULL OR extract_start_date = '' OR extract_start_date = 'daily_batch' THEN 
      SET extract_start_date = theDayBefore;
      SET extract_end_date = theDayBefore;
      SET period = 1;
  END CASE;

  --Clientを検索するために遡るインターバル日数を計算(10日間をバッファとして加算)
  EXECUTE IMMEDIATE format("""
    SELECT DATE_DIFF(DATE '%s', DATE '%s', DAY) + 10;
  """, theDayBefore, extract_start_date)
  INTO interval_days;

  --ログデータから存在しているClientを取得する
  CALL `flightboard_contents.p_f3_1_0_get_some_content_client_ids`(content, CAST(extract_start_date AS DATE), interval_days, clients);

  --Client毎に指定期間のデータの集計を行う
  FOR ids IN (SELECT client from UNNEST(clients) AS client)
  DO
    SELECT ids.client;

    --コンテント集計クエリを実行
    CALL `flightboard_contents.p_f3_1_0_replace_data_agg_some_content_client_nd`(content, ids.client, CAST(extract_start_date AS DATE), CAST(extract_end_date AS DATE), period, false);

    --コンテンツ集計結果格納先テーブル名を設定
    SET destiny_dataset_table = CONCAT("flightboard_contents.t_" , content, "_30d");

    --ログ格納結果を登録
    CALL `flightboard_contents.p_f3_1_1_write_result_logging_agg`(job_id, job_name, content, ids.client, 'daily', extract_start_date, extract_end_date, destiny_dataset_table);
  END FOR;

END;