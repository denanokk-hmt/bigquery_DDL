CREATE OR REPLACE PROCEDURE `flightboard_contents.p_f3_4_5_sql_execute_agg_chained_tags_24h`(
  tmp_table STRING, 
  client STRING, 
  extract_start_date DATE, 
  extract_end_date DATE, 
  period INT64, 
  suffix STRING
)
BEGIN
/*==============================================
STORED PROCEDURE::fligtboard_contents.p_f3_4_5_sql_execute_agg_chained_tags_24h

ARGS::
・tmp_table：ログデータが確保されている一時テーブル名
・client：抽出・集計クライアント
・extract_start_date：抽出・集計開始日
・extract_end_date：抽出・集計終了日
・period：集計期間(範囲)
・suffix："30d" or "12m"

REMARKS::
--■■時間別利用数
・p_sql_aggrigation_ndからCallされる孫プロシージャ
・イベントラベルの集計データをDELETE & INSERT
・suffixが"30d”の場合、dailyデータ(日次データ)
・suffixが"12m"の場合、Mothlyデータ(月次データ)
・指定するtmp_tableでは、クライアント、抽出日は条件済み
==============================================*/

  DECLARE extract_month STRING;
  DECLARE extract_month_start_date DATE;
  DECLARE extract_month_end_date DATE;

  /*----------------------
  ■Daily集計向けDELETE & INSERT INTO
  ----------------------*/
  IF suffix = "30d" THEN

    --集計前に同日同クライアントの集計結果を削除
    EXECUTE IMMEDIATE format("""
      DELETE FROM `flightboard_contents.t_chained_tags_24h_30d`
      WHERE 
        client = "%s"
        AND extract_start_date = "%t"
        AND extract_end_date = "%t"
      ;
    """, client, extract_start_date, extract_end_date);

    --chained_tagsを集計したものをテーブルへ挿入
    EXECUTE IMMEDIATE format("""
      INSERT INTO `flightboard_contents.t_chained_tags_24h_30d`(
        client, extract_start_date, extract_end_date, period, month, date, hour24, count, timestamp
      )
      WITH
      dts AS (
        SELECT 
          CAST(DATETIME(timestamp, "Asia/Tokyo") AS STRING) as ts
        FROM `%s` 
      ),
      tms AS (
        SELECT
          REPLACE(dts.ts, " ", ":"),
          SPLIT(REPLACE(dts.ts, " ", ":"), ":")[offset(0)] AS dt,
          SPLIT(REPLACE(dts.ts, " ", ":"), ":")[offset(1)] AS hour24
        FROM
          dts
      )            
      SELECT
        "%s" AS client,
        CAST("%t" AS DATE) AS extract_start_date,
        CAST("%t" AS DATE) AS extract_end_date, 
        %d AS period,
        SUBSTR(CAST(tms.dt AS STRING), 1, 7) AS month,
        CAST(tms.dt AS DATE) AS date,
        CAST(tms.hour24 AS INT64) AS hour24,
        COUNT(*) AS count,
        CURRENT_TIMESTAMP() AS timestamp
      FROM
        tms
      GROUP BY
        tms.dt, tms.hour24
      ;
    """, tmp_table, client, extract_start_date, extract_end_date, period);

  /*----------------------
  ■Monthly集計向けDELETE & INSERT INTO
  ----------------------*/
  ELSEIF suffix = "12m" THEN

    SET extract_month = LEFT(CAST(extract_start_date AS STRING), 7);
    SET extract_month_start_date = extract_start_date;
    SET extract_month_end_date = extract_end_date;
/*
    --集計前に同期間同クライアントの集計結果を削除
    EXECUTE IMMEDIATE format("""
      DELETE FROM `flightboard_contents.t_specialtag_12m` AS m
      WHERE 
        1=1
        AND m.client = "%s"
        AND extract_month_start_date = "%t"
    """, client, extract_month_start_date);

    --Mothly集計テーブルへ挿入
    EXECUTE IMMEDIATE format("""
      INSERT INTO `flightboard_contents.t_specialtag_12m`(
        client, extract_month, extract_month_start_date, hmt_id, url, query_tags, customer_uuid, count, timestamp
      
      WITH d AS (
        SELECT * FROM `flightboard_contents.t_specialtag_30d` 
        WHERE 1=1
          AND client ="%s"
          AND extract_start_date >= "%t" --start_dateに初日をあてる
          AND extract_start_date <= "%t" --start_dateに最終日をあてる
          AND period = 1
      )
      --COUNTで集計
      SELECT
        "%s" AS client,
        "%s" AS extract_month,
        CAST("%t" AS DATE) AS extract_month_start_date,
        hmt_id,
        url,
        query_tags,
        customer_uuid,
        COUNT(*) AS count,
        CURRENT_TIMESTAMP() AS timestamp,
      FROM d
      GROUP BY
        hmt_id,
        url,
        query_tags,
        customer_uuid
      ORDER BY
        url,
        query_tags
      ;
    """, client, extract_month_start_date, extract_month_end_date, client, extract_month, extract_month_start_date);
*/
  END IF;
END;