CREATE OR REPLACE PROCEDURE `flightboard_contents.p_f3_4_2_sql_execute_agg_buttons_events`(
  tmp_table STRING, 
  client STRING, 
  extract_start_date DATE, 
  extract_end_date DATE, 
  period INT64, 
  suffix STRING
)
BEGIN
/*==============================================
STORED PROCEDURE::fligtboard_contents.p_f3_4_2_sql_execute_agg_buttons_events

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

  DECLARE var_client STRING;
  DECLARE var_start_date DATE;
  DECLARE var_end_date DATE;
  DECLARE var_period INT64;

  SET var_client = client;
  SET var_start_date = extract_start_date;
  SET var_end_date = extract_end_date;
  SET var_period = period;

  /*----------------------
  ■Daily集計向けDELETE & INSERT INTO
  ----------------------*/
  IF suffix = "30d" THEN
      
    --集計前に同日同クライアントの集計結果を削除
    EXECUTE IMMEDIATE format("""
      DELETE FROM `flightboard_contents.t_buttons_events_30d`
      WHERE 
        client = "%s"
        AND extract_start_date = "%t"
        AND extract_end_date = "%t"
      ;
    """, client, extract_start_date, extract_end_date);

    --イベントラベルを集計したものをテーブルへ挿入
    EXECUTE IMMEDIATE format("""
      INSERT INTO `flightboard_contents.t_buttons_events_30d`(
        client,
        extract_start_date,
        extract_end_date,
        period,
        hmt_id,
        buttons_id,
        name,
        text,
        url,
        show_chat_button_count,
        click_chat_button_count,
        timestamp
      )
      WITH clicks AS (
        SELECT
          "%s" AS client,
          CAST("%t" AS DATE) AS extract_start_date,
          CAST("%t" AS DATE) AS extract_end_date, 
          %d AS period,
          hmt_id,
          buttons_id,
          name,
          text,
          url,
          COUNT(*) AS count,
          CURRENT_TIMESTAMP() AS timestamp
        FROM %s
        WHERE
          1=1
          AND dt >= "%t"
          AND dt <= "%t"          
          AND label = "click-chat-button"
        GROUP BY
        hmt_id,
          buttons_id,
          name,
          text,
          url
      )
      , shows AS (
        SELECT
          "%s" AS client,
          CAST("%t" AS DATE) AS extract_start_date,
          CAST("%t" AS DATE) AS extract_end_date, 
          %d AS period,
        hmt_id,
          buttons_id,
          name,
          text,
          url,
          COUNT(*) AS count,
          CURRENT_TIMESTAMP() AS timestamp
        FROM %s
        WHERE
          1=1
          AND dt >= "%t"
          AND dt <= "%t"
          AND label = "show-chat-button"
        GROUP BY
        hmt_id,
          buttons_id,
          name,
          text,
          url
      )
      SELECT
        s.client,
        s.extract_start_date,
        s.extract_end_date, 
        s.period,
        s.hmt_id,
        s.buttons_id,
        s.name,
        s.text,
        s.url,
        IFNULL(SUM(s.count), 0) AS show_chat_button_count,
        IFNULL(SUM(c.count), 0) AS click_chat_button_count,
        CURRENT_TIMESTAMP() AS timestamp
      FROM shows AS s
      LEFT JOIN clicks AS c
      ON
      1 = 1
      AND s.hmt_id = c.hmt_id
        AND s.client = c.client
        AND s.extract_start_date = c.extract_start_date
        AND s.extract_end_date = c.extract_end_date
        AND s.period = c.period
        AND s.buttons_id = c.buttons_id
        AND s.name = c.name
        AND s.text = c.text
        AND s.url = c.url
      GROUP BY
        s.client,
        s.extract_start_date,
        s.extract_end_date, 
        s.period,
      s.hmt_id,
        s.buttons_id,
        s.name,
        s.text,
        s.url
      ;
    """, client, extract_start_date, extract_end_date, period, tmp_table, extract_start_date, extract_end_date, client, extract_start_date, extract_end_date, period, tmp_table, extract_start_date, extract_end_date);

  /*----------------------
  ■Monthly集計向けDELETE & INSERT INTO
  ----------------------*/
  ELSEIF suffix = "12m" THEN
/*
    SET extract_month = LEFT(CAST(extract_start_date AS STRING), 7);
    SET extract_month_start_date = extract_start_date;
    SET extract_month_end_date = extract_end_date;

     --集計前に同期間同クライアントの集計結果を削除
     EXECUTE IMMEDIATE format("""
       DELETE FROM `flightboard_contents.t_buttons_events_12m` AS m
       WHERE 
         1=1
         AND m.client = "%s"
         AND extract_month_start_date = "%t"
     """, client, extract_month_start_date);

     --Mothly集計テーブルへ挿入
     EXECUTE IMMEDIATE format("""
       INSERT INTO `flightboard_contents.t_buttons_events_12m`(
         client, extract_month, extract_month_start_date, buttons_id, text, url, label, count, timestamp
      
       WITH d AS (
         SELECT * FROM `flightboard_contents.t_buttons_events_30d` 
         WHERE 1=1
           AND client ="%s"
           AND extract_start_date >= "%t" start_dateに初日をあてる
           AND extract_start_date <= "%t" start_dateに最終日をあてる
           AND period = 1
       )
       --COUNTで集計
       SELECT
         "%s" AS client,
         "%s" AS extract_month,
         CAST("%t" AS DATE) AS extract_month_start_date,
         buttons_id,
         text,
         label,
         COUNT(*) AS count,
         CURRENT_TIMESTAMP() AS timestamp,
       FROM d
       GROUP BY
         hmt_id,
         url
       ORDER BY
         url
       ;
     """, client, extract_month_start_date, extract_month_end_date, client, extract_month, extract_month_start_date);

     --Mothly集計テーブルへ挿入
     EXECUTE IMMEDIATE format("""
       INSERT INTO `flightboard_contents.t_buttons_events_12m`(
         client, extract_month, extract_month_start_date, url, count, timestamp
       )
       WITH d AS (
         SELECT * FROM `flightboard_contents.t_buttons_events_30d` 
         WHERE 1=1
           AND client ="%s"
           AND extract_start_date >= "%t" start_dateに初日をあてる
           AND extract_start_date <= "%t" start_dateに最終日をあてる
           AND period = 1
       )
       --COUNTで集計
       SELECT
         "%s" AS client,
         "%s" AS extract_month,
         CAST("%t" AS DATE) AS extract_month_start_date,
         url,
         COUNT(*) AS count,
         CURRENT_TIMESTAMP() AS timestamp,
       FROM d
       GROUP BY
         url
       ORDER BY
         url
       ;
     """, client, extract_month_start_date, extract_month_end_date, client, extract_month, extract_month_start_date);
*/
  END IF;
END;