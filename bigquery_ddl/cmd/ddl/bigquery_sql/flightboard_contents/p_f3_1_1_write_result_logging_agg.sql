CREATE OR REPLACE PROCEDURE `flightboard_contents.p_f3_1_1_write_result_logging_agg`(
    job_id STRING, 
    job_name STRING, 
    content STRING, 
    client STRING,
    frequency STRING, 
    extract_start_date STRING, 
    extract_end_date STRING, 
    destiny_dataset_table STRING
)
BEGIN
/*==============================================
STORED PROCEDURE::flightboard_contents.p_f3_1_1_write_result_logging_agg

[ARGS]
job_id STRING::GCP Workflowのjob id, 
job_name STRING::GCP Workflowのjob name, 
content STRING::Logging集計の種類, 
client STRING::ログ抽出対象クライアントコード, 
frequency STRING::ログ抽出サイクル("daily" or "monthly"), 
extract_start_date STRING::ログ抽出対象期間開始日, 
extract_end_date STRING::ログ抽出対象期間終了日, 
destiny_dataset_table STRING::集計挿入先対象ソースID([dataset].[tableid])

[REMARKS]
・ロギングデータの集計格納結果を登録する
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
        AND client = "%s"
        AND DATE(timestamp, "Asia/Tokyo") >= "%s"
        AND DATE(timestamp, "Asia/Tokyo") <= "%s"
      """, destiny_dataset_table, client, extract_start_date, extract_end_date)
    INTO extract_qty;

    --結果を登録
    INSERT INTO `flightboard_contents.t_result_logging_agg`(
      timestamp,
      content,
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
      content,
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