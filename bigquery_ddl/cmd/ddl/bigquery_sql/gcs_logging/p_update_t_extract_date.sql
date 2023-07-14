CREATE OR REPLACE PROCEDURE `gcs_logging.p_update_t_extract_date`(
  jobid STRING,
  extract_start_date STRING, 
  extract_end_date STRING
)
BEGIN
/*==============================================
STORED PROCEDURE::gcs_logging.p_update_t_extract_date

[ARGS]
jobid STRING::WorkflowのJobid
extract_start_date STRING::ログ抽出対象期間開始日, 
extract_end_date STRING::ログ抽出対象期間終了日,

[REMARKS]
Workflowで利用する抽出日を登録する
==============================================*/

  /*-----------------------------
  ■準備
  -----------------------------*/

  DECLARE theDayBefore STRING;

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

  /*-----------------------------
  ■データ更新
  -----------------------------*/
  BEGIN TRANSACTION;

  EXECUTE IMMEDIATE format("""
    UPDATE `gcs_logging.t_extract_date`
    SET 
      jobid = '%s',
      extract_start_date = '%s',
      extract_end_date = '%s',
      timestamp = CURRENT_TIMESTAMP()
    WHERE 1=1;
  """, jobid, extract_start_date, extract_end_date);

  COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
END;