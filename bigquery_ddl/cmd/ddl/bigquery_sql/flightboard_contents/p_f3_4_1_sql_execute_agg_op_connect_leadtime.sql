CREATE OR REPLACE PROCEDURE `flightboard_contents.p_f3_4_1_sql_execute_agg_op_connect_leadtime`(tmp_table STRING, client STRING, extract_start_date DATE, extract_end_date DATE, period INT64, suffix STRING)
BEGIN
/*==============================================
STORED PROCEDURE::fligtboard_contents.p_f3_4_1_sql_execute_agg_op_connect_leadtime

ARGS::
・tmp_table：ログデータが確保されている一時テーブル名
・client：抽出・集計クライアント
・extract_start_date：抽出・集計開始日
・extract_end_date：抽出・集計終了日
・period：集計期間(範囲)
・suffix："30d" or "12m"

REMARKS::
・p_sql_aggrigation_ndからCallされる孫プロシージャ
・オペ接続リードタイムの集計データをDELETE & INSERT
・suffixが"30d”の場合、dailyデータ(日次データ)
・suffixが"12m"の場合、Mothlyデータ(月次データ)
==============================================*/

  DECLARE tmp_table_bot STRING;
  DECLARE tmp_table_op STRING;
  DECLARE extract_month STRING;
  DECLARE extract_month_start_date DATE;
  DECLARE extract_month_end_date DATE;

  /*----------------------
  ■Daily集計向けDELETE & INSERT INTO
  ----------------------*/
  IF suffix = "30d" THEN

    --集計前に同期間同クライアントの集計結果を削除
    EXECUTE IMMEDIATE format("""
      DELETE FROM `flightboard_contents.t_op_connect_leadtime_30d`
      WHERE 
        client = "%s"
        AND extract_start_date = "%t"
        AND extract_end_date = "%t"
      ;
    """, client, extract_start_date, extract_end_date);

    --②bot, ③opのレコードを確保しているTEMPテーブル名を設定
    SET tmp_table_bot = CONCAT(tmp_table, "_bot");
    SET tmp_table_op = CONCAT(tmp_table, "_op");

    --リードタイムを集計したものをテーブルへ挿入
    EXECUTE IMMEDIATE format("""
      INSERT INTO `flightboard_contents.t_op_connect_leadtime_30d`(
        client, extract_start_date, extract_end_date, period, min, percentile1, median, percentile90, max, timestamp
      )
      WITH
        --②tempテーブルのbotレコード
        bot AS (SELECT * FROM %s),

        --③tempテーブルのopレコード
        op AS (SELECT * FROM %s),

        --botとopでtimestamp_diffし応答までの秒数を取得
        diff AS (
          SELECT 
            bot.hmt_id, 
            bot.timestamp_bot_jst, 
            op.timestamp_op_jst,
            timestamp_diff(timestamp_op_jst, timestamp_bot_jst, second) as diff_sec
          FROM 
            bot
          LEFT JOIN op
            ON bot.hmt_id = op.hmt_id 
            AND bot.date = op.date
          WHERE 1= 1
            AND bot.date >= "%t"
            AND bot.date <= "%t"
          ORDER BY
            bot.hmt_id, 
            bot.timestamp_bot_jst, 
            op.timestamp_op_jst
        )

      --percentailで集計
      SELECT
        "%s" AS client,
        CAST("%t" AS DATE) AS extract_start_date,
        CAST("%t" AS DATE) AS extract_end_date,
        %d AS period,
        PERCENTILE_CONT(CAST(diff_sec AS INT64), 0) OVER() AS min,
        PERCENTILE_CONT(CAST(diff_sec AS INT64), 0.01) OVER() AS percentile1,
        PERCENTILE_CONT(CAST(diff_sec AS INT64), 0.5) OVER() AS median,
        PERCENTILE_CONT(CAST(diff_sec AS INT64), 0.9) OVER() AS percentile90,
        PERCENTILE_CONT(CAST(diff_sec AS INT64), 1) OVER() AS max,
        CURRENT_TIMESTAMP() AS timestamp,
      FROM diff
      WHERE
        --前日引き継ぎや、ユーザー切断などを考慮し、diff_secに[0以上、3600以内の条件を設ける)
        diff_sec > 0 AND diff_sec < 3600
      LIMIT 1
      ;
    """, tmp_table_bot, tmp_table_op, extract_start_date, extract_end_date, client, extract_start_date, extract_end_date, period);

  /*----------------------
  ■Monthly集計向けDELETE & INSERT INTO
  ----------------------*/
  ELSEIF suffix = "12m" THEN

    SET extract_month = LEFT(CAST(extract_start_date AS STRING), 7);
    SET extract_month_start_date = extract_start_date;
    SET extract_month_end_date = extract_end_date;

    --集計前に同期間同クライアントの集計結果を削除
    EXECUTE IMMEDIATE format("""
      DELETE FROM `flightboard_contents.t_op_connect_leadtime_12m` AS m
      WHERE 
        1=1
        AND m.client = "%s"
        AND extract_month_start_date = "%t"
      ;
    """, client, extract_month_start_date);

    --リードタイムを集計したものをテーブルへ挿入
    EXECUTE IMMEDIATE format("""
      INSERT INTO `flightboard_contents.t_op_connect_leadtime_12m`(
        client, extract_month, extract_month_start_date, ave_min, ave_percentile1, ave_median, ave_percentile90, ave_max, timestamp
      )
      WITH d AS (
        SELECT * FROM `flightboard_contents.t_op_connect_leadtime_30d` 
        WHERE 1=1
          AND client ="%s"
          AND extract_start_date >= "%t" --start_dateに初日をあてる
          AND extract_start_date <= "%t" --start_dateに最終日をあてる
          AND period = 1
      )
      --averageで集計
      SELECT
        "%s" AS client,
        "%s" AS extract_month,
        CAST("%t" AS DATE) AS extract_month_start_date,
        AVG(min) AS ave_min,
        AVG(percentile1) AS ave_percentile1,
        AVG(median) AS ave_median,
        AVG(percentile90) AS ave_percentile90,
        AVG(max) AS ave_max,
        CURRENT_TIMESTAMP() AS timestamp,
      FROM d
      ;
    """, client, extract_month_start_date, extract_month_end_date, client, extract_month, extract_month_start_date);

  END IF;
END;