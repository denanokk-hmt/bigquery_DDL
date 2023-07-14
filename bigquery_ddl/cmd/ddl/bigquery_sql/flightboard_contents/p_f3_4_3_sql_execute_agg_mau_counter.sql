CREATE OR REPLACE PROCEDURE `flightboard_contents.p_f3_4_3_sql_execute_agg_mau_counter`(
  tmp_table STRING, 
  client STRING, 
  extract_start_date DATE, 
  extract_end_date DATE, 
  period INT64, 
  suffix STRING
)
BEGIN
/*==============================================
STORED PROCEDURE::flightboard_contents.p_f3_4_3_sql_execute_agg_mau_counter

ARGS::
  tmp_table STRING::ログデータが確保されている一時テーブル名, 
  client STRING::抽出・集計クライアント, 
  extract_start_date DATE::抽出・集計開始日, 
  extract_end_date DATE::抽出・集計終了日, 
  period INT64::集計期間(範囲), 
  suffix STRING::"30d" or "12m"

REMARKS::
・p_sql_aggrigation_ndからCallされる孫プロシージャ
・active user counter mauの集計データをDELETE & INSERT
・extract_start_dateからextract_end_date
==============================================*/

  DECLARE extract_month STRING;
  DECLARE extract_month_start_date DATE;
  DECLARE extract_month_end_date DATE;

  /*----------------------
  ■Daily集計向けDELETE & INSERT INTO
  ----------------------*/
  IF suffix = "30d" THEN

    --集計前に同期間同クライアントの集計結果を削除
    EXECUTE IMMEDIATE format("""
      DELETE FROM `flightboard_contents.t_mau_counter_30d`
      WHERE 
        client = "%s"
        AND extract_start_date = "%t"
        AND extract_end_date = "%t"
      ;
    """, client, extract_start_date, extract_end_date);

    --active_user_counterの集計対象月内のデータをテーブルへ挿入
    EXECUTE IMMEDIATE format("""
      INSERT INTO `flightboard_contents.t_mau_counter_30d`(
        client, extract_start_date, extract_end_date, period, count, month, date, timestamp
      )
      WITH auc AS (
        SELECT
          client,
          MAX(count) AS max_count,
          month,
          date
        FROM
          %s
        WHERE
          1=1
          AND client ="%s"
          AND date >= "%t"
          AND date <= "%t"
        GROUP BY
          client,
          month,
          date
        )
      SELECT
        "%s" AS client,
        CAST("%t" AS DATE) AS extract_start_date,
        CAST("%t" AS DATE) AS extract_end_date, 
        %d AS period,
        max_count AS count,
        month,
        date,
        CURRENT_TIMESTAMP() AS timestamp
      FROM
        auc
      ;
    """, tmp_table, client, extract_start_date, extract_end_date, client, extract_start_date, extract_end_date, period);

-- 12mは指定月のmax(count)GROUP BY client, monthを取得する
  /*----------------------
  ■Monthly集計向けDELETE & INSERT INTO
  ----------------------*/
  ELSEIF suffix = "12m" THEN

    SET extract_month = LEFT(CAST(extract_start_date AS STRING), 7);
    SET extract_month_start_date = extract_start_date;
    SET extract_month_end_date = extract_end_date;

    --集計前に同期間同クライアントの集計結果を削除
    EXECUTE IMMEDIATE format("""
      DELETE FROM `flightboard_contents.t_mau_counter_12m` AS m
      WHERE 
        1=1
        AND m.client = "%s"
        AND extract_month_start_date = "%t"
    """, client, extract_month_start_date);

    --Mothly集計テーブルへ挿入
    EXECUTE IMMEDIATE format("""
      INSERT INTO `flightboard_contents.t_mau_counter_12m`(
        client, extract_month, extract_month_start_date, count, month, timestamp
      )
      WITH auc AS (
        SELECT 
          client,
          period,
          max(count) AS count,
          month,
        FROM `flightboard_contents.t_mau_counter_30d` 
        WHERE 1=1
          AND client ="%s"
          AND extract_start_date >= "%t" --start_dateに初日をあてる
          AND extract_start_date <= "%t" --start_dateに最終日をあてる
          AND period = 1
        GROUP BY
          client,
          month,
          period
      )
      SELECT
        "%s" AS client,
        "%s" AS extract_month,
        CAST("%t" AS DATE) AS extract_month_start_date,
        count,
        month,
        CURRENT_TIMESTAMP() AS timestamp,
      FROM auc
      ;
    """, client, extract_month_start_date, extract_month_end_date, client, extract_month, extract_month_start_date);

  END IF;
END