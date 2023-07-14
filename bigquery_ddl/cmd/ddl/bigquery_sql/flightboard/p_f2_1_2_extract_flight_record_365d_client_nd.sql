CREATE OR REPLACE PROCEDURE `flightboard.p_f2_1_2_extract_flight_record_365d_client_nd`( 
  client STRING, 
  extract_start_date STRING, 
  extract_end_date STRING, 
  migration_source STRING
)
BEGIN
/*==============================================
STORED PROCEDURE::flightboard.p_f2_1_2_extract_flight_record_365d_client_nd

[ARGS]
client STRING::ログ抽出対象クライアントコード, 
extract_start_date STRING::ログ抽出対象期間開始日, 
extract_end_date STRING::ログ抽出対象期間終了日,
migration_source STRING::データソース元を強制敵意指定([project].[dataset].[table_id])

[REMARKS]
・flight_recorderからのGCSロギングデータを
　flightboardデータセット上のクライアント別テーブルへ、
　n日分のデータを挿入するプロシージャー
・データをリカバリーしたい場合、3rd引数:migration_tableを指定
==============================================*/

  DECLARE table_id_source STRING;
  DECLARE table_id_destiny STRING;

  --もしも該当クライアント挿入先のテーブルがなければ作成、あれば無視
  CALL `flightboard.p_f2_1_1_create_table_flight_record_365d_client`(client);

  --データソース元テーブルを指定
  SET table_id_source = "gcs_logging.t_flight_record_import";

  --データソース元に指定がある場合(for マイグレーション)
  IF migration_source IS NOT NULL OR migration_source != '' THEN
    SET table_id_source = migration_source;
  END IF;

  --データ挿入先テーブルを指定
  SET table_id_destiny = CONCAT("flightboard.t_flight_record_365d_" , client);

  BEGIN TRANSACTION;

    --指定された日のデータを削除しておく(リカバリー時も安心)
    EXECUTE IMMEDIATE format("""
    DELETE FROM `%s`
    WHERE 
      1=1
      AND DATE(timestamp, "Asia/Tokyo") >= "%s"
      AND DATE(timestamp, "Asia/Tokyo") <= "%s"
    """, table_id_destiny, extract_start_date, extract_end_date);
    
    --指定された日のデータを挿入する
    EXECUTE IMMEDIATE format("""
    INSERT INTO `%s` (
      Cdt,
      jsonPayload,
      timestamp
    )
    SELECT
      CURRENT_TIMESTAMP() AS Cdt,
      jsonPayload,
      timestamp
    FROM `%s` AS v
    WHERE
      1=1
      AND jsonPayload.common.client = "%s"
      AND DATE(timestamp, "Asia/Tokyo") >= "%s"
      AND DATE(timestamp, "Asia/Tokyo") <= "%s"
    """, table_id_destiny, table_id_source, client, extract_start_date, extract_end_date);

  COMMIT TRANSACTION;

  EXCEPTION WHEN ERROR THEN
    ROLLBACK TRANSACTION;
END;