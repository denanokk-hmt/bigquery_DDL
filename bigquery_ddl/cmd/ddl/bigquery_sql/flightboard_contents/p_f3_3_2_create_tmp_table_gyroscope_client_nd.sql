CREATE OR REPLACE PROCEDURE `flightboard_contents.p_f3_3_2_create_tmp_table_gyroscope_client_nd`(
  content STRING, 
  client STRING, 
  extract_start_date DATE, 
  extract_end_date DATE, 
  OUT tmp_table_name STRING
)
BEGIN
/*==============================================
STORED PROCEDURE::fligtboard_contents.p_f3_3_2_create_tmp_table_gyroscope_client_nd

ARGS::
・content：抽出・集計したいコンテントを指定
・client：抽出・集計クライアント
・extract_start_date：抽出・集計開始日
・extract_end_date：抽出・集計終了
・tmp_table_name：ログデータを格納した一時テーブル名を返却

REMARKS::
・gyroscopeの指定したクライアントの365dテーブルから、
　指定期間のレコードをTEMPへ格納する
==============================================*/

  DECLARE table_id_source STRING;

  --ログデータ抽出元と抽出先を指定
  SET table_id_source = CONCAT("flightboard.t_gyroscope_365d_", client);
  SET tmp_table_name = CONCAT("TmpTbl_Gyroscope_", client);

  --TEMP Tableに該当クライアントの365dフライトレコードを収納
  IF content = "pageview" THEN
    EXECUTE IMMEDIATE format("""
      CREATE TEMP TABLE IF NOT EXISTS %s AS (
        SELECT
          -- SPLIT("?"): URLのGETパラメータを削除
          -- SPLIT("#"): URLのフラグメントを削除
          -- REGEXP_REPLACE関数：URLの最後に"/"があれば、”/”を削除
          REGEXP_REPLACE(SPLIT(SPLIT(url, "?")[OFFSET(0)], "#")[OFFSET(0)], '/$', '') AS url,
          hmt_id,
          DATETIME(timestamp, "Asia/Tokyo") AS datetime_jst,
        FROM 
          %s
        WHERE
          hmt_id IS NOT NULL
          AND label = "pageview"
          AND DATE(timestamp, "Asia/Tokyo") >= "%t"
          AND DATE(timestamp, "Asia/Tokyo") <= "%t"
      );
    """, tmp_table_name, table_id_source, extract_start_date, extract_end_date);

  ELSEIF content = "buttons_events" THEN
    EXECUTE IMMEDIATE format("""
      CREATE TEMP TABLE IF NOT EXISTS %s AS (
        SELECT
          hmt_id,
          JSON_EXTRACT_SCALAR(value, '$.id') AS buttons_id,
          JSON_EXTRACT_SCALAR(value, '$.name') AS name,
          JSON_EXTRACT_SCALAR(value, '$.text') AS text,
          -- SPLIT("?"): URLのGETパラメータを削除
          -- SPLIT("#"): URLのフラグメントを削除
          -- REGEXP_REPLACE関数：URLの最後に"/"があれば、”/”を削除
          REGEXP_REPLACE(SPLIT(SPLIT(url, "?")[OFFSET(0)], "#")[OFFSET(0)], '/$', '') AS url,
          label,
          DATE(timestamp, "Asia/Tokyo") AS dt,
        FROM 
          %s
        WHERE
          1=1
          AND hmt_id IS NOT NULL
          AND JSON_EXTRACT_SCALAR(VALUE, '$.id') IS NOT NULL
          AND label IN ("show-chat-button", "click-chat-button")
          AND DATE(timestamp, "Asia/Tokyo") >= "%t"
          AND DATE(timestamp, "Asia/Tokyo") <= "%t"
      );
    """, tmp_table_name, table_id_source, extract_start_date, extract_end_date);
  ELSE
    EXECUTE IMMEDIATE format("""
      CREATE TEMP TABLE IF NOT EXISTS %s AS (
        SELECT *
        FROM `%s` 
        WHERE 
          1=1
          AND DATE(timestamp, "Asia/Tokyo") >= "%t"
          AND DATE(timestamp, "Asia/Tokyo") <= "%t"
      );
    """, tmp_table_name, table_id_source, extract_start_date, extract_end_date);
  END IF;

END;