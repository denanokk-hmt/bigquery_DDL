CREATE OR REPLACE PROCEDURE `flightboard.p_f2_2_2_create_tmp_table_gyroscope_365d_extract_date`(
  extract_start_date STRING, 
  extract_end_date STRING, 
  migration_source STRING, 
  OUT tmp_table_name STRING
)
BEGIN
/*==============================================
STORED PROCEDURE::flightboard.p_f2_2_2_create_tmp_table_gyroscope_365d_extract_date

[ARGS]
client STRING::ログ抽出対象クライアントコード, 
extract_start_date STRING::ログ抽出対象期間開始日, 
extract_end_date STRING::ログ抽出対象期間終了日,
migration_source STRING::データソース元を強制敵意指定([project].[dataset].[table_id]),
OUT tmp_table_name STRING::ここで作成された一時テーブル名を返却

[REMARKS]
・指定したクライアントのgyroscope_365dテーブルから、期間レコードをTEMPへ格納する
・Clientでは切り分けないので、全Client向けのTempTableを24時間参照可能にして、
　コストを大幅に下げる
==============================================*/

  DECLARE table_id_source STRING;

  --データソース元テーブルを指定
  SET table_id_source = "gcs_logging.t_gyroscope_import";

  IF migration_source IS NOT NULL OR migration_source != '' THEN 
    SET table_id_source = migration_source;
  END IF;

  --TEMPテーブル名を指定(TmpTbl_Gyroscope_[yyyymmdd]_[yyyymmdd])
  SET tmp_table_name = CONCAT("TmpTbl_Gyroscope_", REPLACE(extract_start_date, "-", ""), "_", REPLACE(extract_end_date, "-", ""));

  --TEMP Tableにログを収納
  EXECUTE IMMEDIATE format("""
    CREATE TEMP TABLE IF NOT EXISTS %s AS (
      SELECT *
      FROM `%s` 
      WHERE 
        1=1
        AND DATE(timestamp, "Asia/Tokyo") >= "%s"
        AND DATE(timestamp, "Asia/Tokyo") <= "%s"
    );
  """, tmp_table_name, table_id_source, extract_start_date, extract_end_date);

  --TEMPテーブルをRead(検証用)
  --SELECT * FROM TmpTbl_Gyroscope_;
  
END;