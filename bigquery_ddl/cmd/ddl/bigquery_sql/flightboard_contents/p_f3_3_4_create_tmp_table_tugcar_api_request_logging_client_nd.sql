CREATE OR REPLACE PROCEDURE `flightboard_contents.p_f3_3_4_create_tmp_table_tugcar_api_request_logging_client_nd`(client STRING, extract_start_date DATE, extract_end_date DATE, INOUT tmp_table_name STRING)
BEGIN
/*==============================================
STORED PROCEDURE::fligtboard_contents.p_f3_3_4_create_tmp_table_tugcar_api_request_logging_client_nd

ARGS::
・client：抽出・集計クライアント
・extract_start_date：抽出・集計開始日
・extract_end_date：抽出・集計終了
・tmp_table_name：ログデータを格納した一時テーブル名を返却

REMARKS::
・gcs_logging.t_tugcar_api_request_loggingテーブルから、
　指定期間のレコードをTEMPへ格納する
==============================================*/

  DECLARE table_id_source STRING;

  --gyroscope
  SET table_id_source = "gcs_logging.t_tugcar_api_request_logging_import";
  SET tmp_table_name = CONCAT("TmpTbl_TugCarAPIRequestLogging_", client);

  --TEMP Tableに該当クライアントのactive user counterレコードを収納
  EXECUTE IMMEDIATE format("""
    CREATE TEMP TABLE IF NOT EXISTS %s AS (
      SELECT
        client_id AS client,
        hmt_id,
        current_url AS url,
        query_tags,
        customer_uuid,
        DATE(timestamp, "Asia/Tokyo") AS date
      FROM 
        %s
      WHERE
        1=1
        AND client_id = "%s"
        AND DATE(timestamp, "Asia/Tokyo") >= "%t"
        AND DATE(timestamp, "Asia/Tokyo") <= "%t"
        AND url_path = "/hmt/attachment/search/SpecialTag" 
    );
  """, tmp_table_name, table_id_source, client, extract_start_date, extract_end_date);

END