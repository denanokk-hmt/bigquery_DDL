CREATE OR REPLACE PROCEDURE `gcs_logging.p_delete_some_content_import_nd`( 
  jobid STRING,
  content STRING, 
  extract_start_date STRING, 
  extract_end_date STRING
)
BEGIN
/*==============================================
STORED PROCEDURE::gcs_logging.p_delete_some_content_import_nd

[ARGS]
jobid STRING::Workflow Jobid,
content STRING::レコード削除対象コンテント名,
extract_start_date STRING::ログ抽出対象期間開始日, 
extract_end_date STRING::ログ抽出対象期間終了日,

[REMARKS]
・GCSロギングデータの抽出日期間のレコードを削除する
==============================================*/

  DECLARE table_id_destiny STRING;

  --データ挿入先テーブルを指定
  SET table_id_destiny = CONCAT("gcs_logging.t_", content , "_import");

  BEGIN TRANSACTION;

    --指定された日のデータを削除しておく(リカバリー時も安心)
    EXECUTE IMMEDIATE format("""
    DELETE FROM `%s`
    WHERE 
      1=1
      AND DATE(timestamp, "Asia/Tokyo") >= "%s"
      AND DATE(timestamp, "Asia/Tokyo") <= "%s"
    """, table_id_destiny, extract_start_date, extract_end_date);

  COMMIT TRANSACTION;

  EXCEPTION WHEN ERROR THEN
    ROLLBACK TRANSACTION;
END;