CREATE OR REPLACE PROCEDURE `flightboard.p_f2_3_0_write_result_logging_extract`(
    job_id STRING, 
    job_name STRING, 
    kind STRING, 
    client STRING,
    frequency STRING, 
    extract_start_date STRING, 
    extract_end_date STRING, 
    destiny_dataset_table STRING
)
BEGIN
/*==============================================
STORED PROCEDURE::flightboard.p_f2_3_0_write_result_logging_extract

[ARGS]
job_id STRING::GCP Workflowのjob id, 
job_name STRING::GCP Workflowのjob name, 
kind STRING::Logging抽出の種類("flight_record" or "gyroscope"), 
client STRING::ログ抽出対象クライアントコード, 
frequency STRING::ログ抽出サイクル("daily" or "monthly"), 
extract_start_date STRING::ログ抽出対象期間開始日, 
extract_end_date STRING::ログ抽出対象期間終了日, 
destiny_dataset_table STRING::ログ挿入先対象ソースID([dataset].[tableid])

[REMARKS]
・flight_recorderやgyroscopeのロギングデータの格納結果を登録する
==============================================*/

  DECLARE extract_qty INT64;

  BEGIN TRANSACTION;

    --結果を取得
    EXECUTE IMMEDIATE format("""
      SELECT
        COUNT(*) 
      FROM `%s` AS v
      WHERE
        1=1
        AND DATE(timestamp, "Asia/Tokyo") >= "%s"
        AND DATE(timestamp, "Asia/Tokyo") <= "%s"
      """, destiny_dataset_table, extract_start_date, extract_end_date)
    INTO extract_qty;

    --結果を登録
    INSERT INTO `flightboard.t_result_logging_extract`(
      timestamp,
      kind,
      job_id,
      job_name,
      frequency,
      client,
      extract_start_date,
      extract_end_date,
      extract_qty
    )
    VALUES(
      CURRENT_TIMESTAMP(),
      kind,
      job_id,
      job_name,
      frequency,
      client,
      CAST(extract_start_date AS DATE),
      CAST(extract_end_date AS DATE),
      extract_qty
    );

  COMMIT TRANSACTION;

  EXCEPTION WHEN ERROR THEN
    ROLLBACK TRANSACTION;
END;