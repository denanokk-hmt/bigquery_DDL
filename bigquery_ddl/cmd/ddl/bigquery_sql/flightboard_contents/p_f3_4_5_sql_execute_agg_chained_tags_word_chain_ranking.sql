CREATE OR REPLACE PROCEDURE `flightboard_contents.p_f3_4_5_sql_execute_agg_chained_tags_word_chain_ranking`(
  tmp_table STRING, 
  client STRING, 
  extract_start_date DATE, 
  extract_end_date DATE, 
  period INT64, 
  suffix STRING
)
BEGIN
/*==============================================
STORED PROCEDURE::fligtboard_contents.p_f3_4_5_sql_execute_agg_chained_tags_word_chain_ranking

ARGS::
・tmp_table：ログデータが確保されている一時テーブル名
・client：抽出・集計クライアント
・extract_start_date：抽出・集計開始日
・extract_end_date：抽出・集計終了日
・period：集計期間(範囲)
・suffix："30d" or "12m"

REMARKS::
--■■UUワードチェーンRanking
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
      DELETE FROM `flightboard_contents.t_chained_tags_word_chain_ranking_30d`
      WHERE 
        client = "%s"
        AND extract_start_date = "%t"
        AND extract_end_date = "%t"
      ;
    """, client, extract_start_date, extract_end_date);

    --chained_tagsを集計したものをテーブルへ挿入
    EXECUTE IMMEDIATE format("""
      INSERT INTO `flightboard_contents.t_chained_tags_word_chain_ranking_30d`(
        client, extract_start_date, extract_end_date, period, month, rank, word_chain, count, hmt_id, timestamp
      )
      WITH
      rwl AS (
        SELECT 
          DATE(timestamp, "Asia/Tokyo") as dt,
          hmt_id,
          ARRAY_TO_STRING(related_words_log, ",") AS word_chain,             
        FROM `%s` 
        WHERE gqty > 1 
      ),
      cnt AS (
      SELECT
        SUBSTR(CAST(rwl.dt AS STRING), 1, 7) AS month,
        hmt_id,
        word_chain,
        COUNT(hmt_id) AS count
      FROM rwl
      GROUP BY
        rwl.dt,
        hmt_id,            
        word_chain
      )
      SELECT
        "%s" AS client,
        CAST("%t" AS DATE) AS extract_start_date,
        CAST("%t" AS DATE) AS extract_end_date, 
        %d AS period,            
        month,
        RANK() OVER (ORDER BY count DESC) AS rank,
        word_chain,
        count,
        hmt_id,
        CURRENT_TIMESTAMP() AS timestamp
      FROM cnt
      ORDER BY rank
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