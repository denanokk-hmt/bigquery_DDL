CREATE OR REPLACE PROCEDURE `flightboard_contents.p_f3_4_1_sql_execute_agg_cust_msg`(tmp_table STRING, client STRING, extract_start_date DATE, extract_end_date DATE, period INT64, suffix STRING)
BEGIN
/*==============================================
STORED PROCEDURE::fligtboard_contents.p_f3_4_1_sql_execute_agg_cust_msg

ARGS::
・tmp_table：ログデータが確保されている一時テーブル名
・client：抽出・集計クライアント
・extract_start_date：抽出・集計開始日
・extract_end_date：抽出・集計終了日
・period：集計期間(範囲)
・suffix："30d" or "12m"

REMARKS::
・p_sql_aggrigation_ndからCallされる孫プロシージャ
・custメッセージの集計データをDELETE & INSERT
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

    --集計前に同期間同クライアントの集計結果を削除
    EXECUTE IMMEDIATE format("""
      DELETE FROM `flightboard_contents.t_cust_msg_30d`
      WHERE 
        client = "%s"
        AND extract_start_date = "%t"
        AND extract_end_date = "%t"
      ;
    """, client, extract_start_date, extract_end_date);

    --集計したものをテーブルへ挿入
    EXECUTE IMMEDIATE format("""

      INSERT INTO `flightboard_contents.t_cust_msg_30d`(
        client, extract_start_date, extract_end_date, period, current_url, send_to, talk_cust_type, talk_cust_message, talk_cust_value, count, timestamp
      )
      WITH cust_tap AS (
        SELECT
          SUBSTR(CAST(date AS STRING), 1, 7) AS month,
          date,
          client,
          hmt_id,
          timestamp_jst,
          current_url,
          send_to,
          talk_cust_type,
          talk_cust_message,
          talk_cust_value,
        FROM
          %s
        WHERE
          1=1
          --AND talk_cust_value IS NOT NULL --textタイプを除去
          --AND messages[OFFSET(0)].talk.content.message != "connect_operator" --OP接続commandを除外
          --AND response_body_status_msg != "Success user msg published." --C2O[POST_MESSAGE]を除外
        )
      SELECT
        "%s" AS client,
        CAST("%t" AS DATE) AS extract_start_date,
        CAST("%t" AS DATE) AS extract_end_date,
        %d AS period,
        current_url,
        send_to,
        talk_cust_type,
        talk_cust_message,
        talk_cust_value,
        COUNT(*) AS count,
        CURRENT_TIMESTAMP() AS timestamp
      FROM
        cust_tap
      GROUP BY
        current_url,
        send_to,
        talk_cust_type,
        talk_cust_message,
        talk_cust_value
      ;
    """, tmp_table, client, extract_start_date, extract_end_date, period);

  /*----------------------
  ■Monthly集計向けDELETE & INSERT INTO
  ----------------------*/
  ELSEIF suffix = "12m" THEN

    SET extract_month = LEFT(CAST(extract_start_date AS STRING), 7);
    SET extract_month_start_date = extract_start_date;
    SET extract_month_end_date = extract_end_date;

    --集計前に同期間同クライアントの集計結果を削除
    EXECUTE IMMEDIATE format("""
      DELETE FROM `flightboard_contents.t_cust_msg_12m` AS m
      WHERE 
        1=1
        AND m.client = "%s"
        AND extract_month_start_date = "%t"
      ;
    """, client, extract_month_start_date);

    --Mothly集計テーブルをテーブルへ挿入
    EXECUTE IMMEDIATE format("""
      INSERT INTO `flightboard_contents.t_cust_msg_12m`(
        client, extract_month, extract_month_start_date, current_url, send_to, talk_cust_type, talk_cust_message, talk_cust_value, count, timestamp
      )
      WITH d AS (
        SELECT * FROM `flightboard_contents.t_cust_msg_30d` 
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
        current_url,
        send_to,
        talk_cust_type,
        talk_cust_message,
        talk_cust_value,
        COUNT(*) AS count,
        CURRENT_TIMESTAMP() AS timestamp,
      FROM d
      GROUP BY
        current_url,
        send_to,
        talk_cust_type,        
        talk_cust_message,
        talk_cust_value
      ORDER BY
        current_url,
        talk_cust_value
      ;
    """, client, extract_month_start_date, extract_month_end_date, client, extract_month, extract_month_start_date);

  END IF;
END;