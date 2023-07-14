CREATE OR REPLACE PROCEDURE `flightboard_contents.p_f3_3_5_create_tmp_table_chained_tags_logging_client_nd`(
  client STRING, 
  extract_start_date DATE, 
  extract_end_date DATE, 
  INOUT tmp_table_name STRING
)
BEGIN
/*==============================================
STORED PROCEDURE::fligtboard_contents.p_f3_3_5_create_tmp_table_chained_tags_logging_client_nd

ARGS::
・client：抽出・集計クライアント
・extract_start_date：抽出・集計開始日
・extract_end_date：抽出・集計終了
・tmp_table_name：ログデータを格納した一時テーブル名を返却

REMARKS::
・gcs_logging.t_chained_tags_logging_import
　指定期間のレコードをTEMPへ格納する
※クライアント数が増え、ログが増加した際には、flight_recordなどのように、クライアント別テーブルへ
格納し、集計テーブルの準備を検討する
==============================================*/

  DECLARE table_id_source STRING;

  --gyroscope
  SET table_id_source = "gcs_logging.t_chained_tags_logging_import";
  SET tmp_table_name = CONCAT("TmpTbl_ChainedTagsLogging_", client);

  --TEMP Tableに該当クライアントのactive user counterレコードを収納
  EXECUTE IMMEDIATE format("""
    CREATE TEMP TABLE IF NOT EXISTS %s AS (
      SELECT
        timestamp,
        client_id,
        customer_uuid,
        hmt_id,
        atid,
        ddid,
        gid,
        gqty,
        related_unixtime,
        related_words_log,
        g_sort,
        selected_item_id,
        selected_item_name
      FROM 
        %s
      WHERE 1=1
        AND client_id = "%s"
        AND DATE(timestamp, "Asia/Tokyo") >= "%t"
        AND DATE(timestamp, "Asia/Tokyo") <= "%t"
    );
  """, tmp_table_name, table_id_source, client, extract_start_date, extract_end_date);

END