CREATE OR REPLACE PROCEDURE `flightboard.p_f2_1_0_batch_extract_flight_record_365d_client_nd_clients`(
  job_id STRING, 
  job_name STRING,
  extract_start_date STRING, 
  extract_end_date STRING
)
BEGIN
/*==============================================
STORED PROCEDURE::flightboard.p_f2_1_0_batch_extract_flight_record_365d_client_nd_clients

[ARGS]
job_id STRING::GCP Workflowのjob id, 
job_name STRING::GCP Workflowのjob name, 
extract_start_date STRING::ログ抽出対象期間開始日, 
extract_end_date STRING::ログ抽出対象期間終了日

・flight_recorderからのGCSロギングデータを
　flightboard上に、クライアント別テーブルを作成、n日分のデータを挿入するための
　プロシージャーを呼ぶための親プロシージャの
　親プロシージャ--> バッチ実行時にログのレコードからCliendIDを導き出し、
　そのClientID分のバッチを回すためのバッチ
　（バッチ実行用）
==============================================*/

  DECLARE theDayBefore STRING;
  DECLARE destiny_dataset_table STRING;
  DECLARE ddiff INT64;

  --前日を取得
  SET theDayBefore = CAST((
    SELECT CURRENT_DATE("Asia/Tokyo") - 1
  ) AS STRING);

  --抽出日を決定(抽出開始日に日付指定がない[=Dailyバッチ運用]場合、前日を設定する)
  CASE 
    WHEN extract_start_date IS NULL OR extract_start_date = '' OR extract_start_date = 'daily_batch' THEN 
      SET extract_start_date = theDayBefore;
      SET extract_end_date = theDayBefore;
  END CASE;

  --Clientを検索するために遡るインターバル日数を計算(10日間をバッファとして加算)
  EXECUTE IMMEDIATE format("""
    SELECT DATE_DIFF(DATE '%s', DATE '%s', DAY) + 10;
  """, theDayBefore, extract_start_date)
  INTO ddiff;

  --Client別にデータをExtractを行う
  FOR ids IN (SELECT client FROM `gcs_logging.tf_get_flight_record_clients`(CAST(extract_start_date AS DATE), ddiff))
  DO
    --ログを各Clientテーブルへ挿入
    CALL `flightboard.p_f2_1_2_extract_flight_record_365d_client_nd`(ids.client, extract_start_date, extract_end_date, NULL);

    --ログ格納先テーブル名を設定
    SET destiny_dataset_table = CONCAT("flightboard.t_flight_record_365d_" , ids.client);

    --ログ格納結果を登録
    CALL `flightboard.p_f2_3_0_write_result_logging_extract`(job_id, job_name, "flight_record", ids.client, 'daily', extract_start_date, extract_end_date, destiny_dataset_table);
  END FOR;

END;