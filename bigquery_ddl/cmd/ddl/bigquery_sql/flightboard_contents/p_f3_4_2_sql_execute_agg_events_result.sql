CREATE OR REPLACE PROCEDURE `flightboard_contents.p_f3_4_2_sql_execute_agg_events_result`(tmp_table STRING, client STRING, extract_start_date DATE, extract_end_date DATE, period INT64, suffix STRING)
BEGIN
/*==============================================
STORED PROCEDURE::fligtboard_contents.p_f3_4_2_sql_execute_agg_events_result

ARGS::
・tmp_table：ログデータが確保されている一時テーブル名
・client：抽出・集計クライアント
・extract_start_date：抽出・集計開始日
・extract_end_date：抽出・集計終了日
・period：集計期間(範囲)
・suffix："30d" or "12m"

REMARKS::
・p_sql_aggrigation_ndからCallされる孫プロシージャ
・イベントラベルの集計データをDELETE & INSERT
・suffixが"30d”の場合、dailyデータ(日次データ)
・suffixが"12m"の場合、Mothlyデータ(月次データ)
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
      DELETE FROM `flightboard_contents.t_events_result_30d`
      WHERE 
        client = "%s"
        AND extract_start_date = "%t"
        AND extract_end_date = "%t"
      ;
    """, client, extract_start_date, extract_end_date);

    --イベントラベルを集計したものをテーブルへ挿入
    EXECUTE IMMEDIATE format("""
      INSERT INTO `flightboard_contents.t_events_result_30d`(
        client, extract_start_date, extract_end_date, period, label, count, timestamp
      )
      SELECT
        "%s" AS client,
        CAST("%t" AS DATE) AS extract_start_date,
        CAST("%t" AS DATE) AS extract_end_date, 
        %d AS period,
        label,
        COUNT(*) AS count,
        CURRENT_TIMESTAMP() AS timestamp
      FROM %s
      WHERE
        1=1
        AND DATE(timestamp, "Asia/Tokyo") >= "%t"
        AND DATE(timestamp, "Asia/Tokyo") <= "%t"
      GROUP BY
        label
      ;
    """, client, extract_start_date, extract_end_date, period, tmp_table, extract_start_date, extract_end_date);

  /*----------------------
  ■Monthly集計向けDELETE & INSERT INTO
  ----------------------*/
  ELSEIF suffix = "12m" THEN

    SET extract_month = LEFT(CAST(extract_start_date AS STRING), 7);
    SET extract_month_start_date = extract_start_date;
    SET extract_month_end_date = extract_end_date;

    --集計前に同期間同クライアントの集計結果を削除
    EXECUTE IMMEDIATE format("""
      DELETE FROM `flightboard_contents.t_events_result_12m` AS m
      WHERE 
        1=1
        AND m.client = "%s"
        AND extract_month_start_date = "%t"
    """, client, extract_month_start_date);

    --Mothly集計テーブルへ挿入
    EXECUTE IMMEDIATE format("""
      INSERT INTO `flightboard_contents.t_events_result_12m`(
        client, extract_month, extract_month_start_date, label, count, timestamp
      
      WITH d AS (
        SELECT * FROM `flightboard_contents.t_events_result_30d` 
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
        label,
        COUNT(*) AS count,
        CURRENT_TIMESTAMP() AS timestamp,
      FROM d
      GROUP BY
        label
      ORDER BY
        label
      ;
    """, client, extract_month_start_date, extract_month_end_date, client, extract_month, extract_month_start_date);

    --Mothly集計テーブルへ挿入
    EXECUTE IMMEDIATE format("""
      INSERT INTO `flightboard_contents.t_events_result_12m`(
        client, extract_month, extract_month_start_date, label, count, timestamp
      )
      WITH d AS (
        SELECT * FROM `flightboard_contents.t_events_result_30d` 
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
        label,
        COUNT(*) AS count,
        CURRENT_TIMESTAMP() AS timestamp,
      FROM d
      GROUP BY
        label
      ORDER BY
        label
      ;
    """, client, extract_month_start_date, extract_month_end_date, client, extract_month, extract_month_start_date);

  END IF;
END;