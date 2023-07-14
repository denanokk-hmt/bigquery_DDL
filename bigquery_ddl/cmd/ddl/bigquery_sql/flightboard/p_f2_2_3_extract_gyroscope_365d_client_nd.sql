CREATE OR REPLACE PROCEDURE `flightboard.p_f2_2_3_extract_gyroscope_365d_client_nd`(
  client STRING, 
  extract_start_date STRING, 
  extract_end_date STRING,
  migration_source STRING,
  EXAM BOOL
)
BEGIN
/*==============================================
STORED PROCEDURE::flightboard.p_f2_2_3_extract_gyroscope_365d_client_nd

[ARGS]
client STRING::ログ抽出対象クライアントコード, 
extract_start_date STRING::ログ抽出対象期間開始日, 
extract_end_date STRING::ログ抽出対象期間終了日,
migration_source STRING::データソース元を強制敵意指定([project].[dataset].[table_id]),
EXAM：TRUEの場合、extarctされたデータを取得して終了する

[REMARKS]
・gyroscopeerからのGCSロギングデータを
　flightboardデータセット上のクライアント別テーブルへ、
　n日分のデータを挿入するプロシージャー
==============================================*/

  DECLARE tmp_table STRING;
  DECLARE table_id_source STRING;
  DECLARE table_id_destiny STRING;

  /*-----------------------------
  ■集計事前準備（箱、データ）
  -----------------------------*/

  --もしも該当クライアントのテーブルがなければ作成、あれば無視
  CALL `flightboard.p_f2_2_1_create_table_gyroscope_365d_client`(client);

  --指定したClientのgyroscopeのレコードを抽出し、TEMPテーブルへ収納する
  CALL `flightboard.p_f2_2_2_create_tmp_table_gyroscope_365d_extract_date`(extract_start_date, extract_end_date, migration_source, tmp_table);

  --レコードが取得出来なかった場合(tmp_tableがNullになる)、処理を中断
  IF tmp_table IS NULL THEN
    SELECT "no records.";
    RETURN;
  END IF;

  /*-----------------------------
  ■検証用
  -----------------------------*/
  IF EXAM = TRUE THEN
    EXECUTE IMMEDIATE format("""  SELECT * FROM %s; """, tmp_table);
    RETURN;
  END IF;

  /*-----------------------------
  ■データ抽出
  -----------------------------*/

  --データソース元テーブルを指定
  SET table_id_source = tmp_table;

  --データ挿入先テーブルを指定
  SET table_id_destiny = CONCAT("flightboard.t_gyroscope_365d_" , client);

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
    INSERT INTO `%s`
    (
      cdt,
      host,
      url,
      path,
      ip,
      user_agent,
      referrer,      
      client_id,
      customer_uuid,
      customer_id,
      hmt_id,
      category_id,
      type,
      id,
      label,
      value,
      wy_event,
      timestamp
    )
    SELECT
      CURRENT_TIMESTAMP() AS cdt,
      host,
      url,
      path,
      ip,
      user_agent,
      referrer,      
      client_id,
      customer_uuid,
      customer_id,
      hmt_id,
      category_id,
      type,
      id,
      label,
      value,
      wy_event,
      timestamp
    FROM %s
    WHERE
      1=1
      AND client_id = "%s"
      AND DATE(timestamp, "Asia/Tokyo") >= "%s"
      AND DATE(timestamp, "Asia/Tokyo") <= "%s"
    """, table_id_destiny, table_id_source, client, extract_start_date, extract_end_date);

  COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
END;