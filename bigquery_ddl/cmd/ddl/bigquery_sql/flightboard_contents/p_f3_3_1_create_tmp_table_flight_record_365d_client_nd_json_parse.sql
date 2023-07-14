CREATE OR REPLACE PROCEDURE `flightboard_contents.p_f3_3_1_create_tmp_table_flight_record_365d_client_nd_json_parse`(client STRING, extract_start_date DATE, extract_end_date DATE, extract_apis ARRAY<STRING>, INOUT tmp_table_name STRING)
BEGIN
/*==============================================
STORED PROCEDURE::fligtboard_contents.p_f3_3_1_create_tmp_table_flight_record_365d_client_nd_json_parse

ARGS::
・client：抽出・集計クライアント
・extract_start_date：抽出・集計開始日
・extract_end_date：抽出・集計終了日
・extract_apis：抽出対象メッセージを配列にして
・tmp_table_name：
    NULL OK→TmpTbl_FlightRecordが設定される
    →処理の中で決められたTEMPテーブル名が返される

REMARKS::
・指定したクライアントのflight_record_65dテーブルから、指定期間で抽出したものを
　JSONパースしたものをTEMPへ格納する
==============================================*/

  DECLARE table_id_source STRING;
  DECLARE extract_api_str STRING;

  --挿入先テンプテーブル名を指定::TmpTbl_FlightRecord_[client]_[yyyymmdd]_[yyyymmdd]
  IF tmp_table_name IS NULL OR tmp_table_name = '' THEN
    SET tmp_table_name = "TmpTbl_FlightRecord";
  END IF;
  SET tmp_table_name = ARRAY_TO_STRING([tmp_table_name, client, REPLACE(CAST(extract_start_date AS STRING), "-", ""), REPLACE(CAST(extract_end_date AS STRING), "-", "")], "_");

  --取得元ソーステーブル名を指定::t_flight_record_365d_[client]
  SET table_id_source = CONCAT("flightboard.t_flight_record_365d_", client);

  --conditionで正規表現検索させるために結合
  SET extract_api_str = ARRAY_TO_STRING(extract_apis, ","); 

  --TEMP Tableに該当クライアントの365dフライトレコードを収納
  EXECUTE IMMEDIATE format("""
    CREATE TEMP TABLE IF NOT EXISTS %s AS (
    WITH
      --t_flight_record_365d_[client]から抽出
      fr AS (
        SELECT *
        FROM `%s` 
        WHERE 
          1=1
          AND DATE(timestamp, "Asia/Tokyo") >= "%t"
          AND DATE(timestamp, "Asia/Tokyo") <= "%t"
      )

      --抽出したレコードのjsonPayloadをJSONパース
      ,jp AS (
        SELECT
          --common系
          jsonPayload.common.customer_uuid,
          jsonPayload.common.hmt_id,
          DATETIME(timestamp, "Asia/Tokyo") AS timestamp_jst,
          DATE(timestamp, 'Asia/Tokyo') AS date,
          jsonPayload.common.client AS client,
          jsonPayload.common.api,
          --request.body系をパース
          jsonPayload.request.body AS req_body,
          JSON_EXTRACT_SCALAR(jsonPayload.request.body, '$.send_to') AS send_to,
          CASE 
            WHEN jsonPayload.common.api = "POST_IMAGE" THEN
              JSON_EXTRACT_SCALAR(jsonPayload.request.body, '$.current_url')
            WHEN jsonPayload.common.api = "POST_MESSAGE" THEN
              JSON_EXTRACT_SCALAR(JSON_EXTRACT(JSON_EXTRACT(jsonPayload.request.body, '$.talk'), '$.content'), '$.current_url')
            ELSE NULL
          END AS current_url,
          JSON_EXTRACT_SCALAR(JSON_EXTRACT(jsonPayload.request.body, '$.talk'), '$.type') AS talk_cust_type,
          JSON_EXTRACT_SCALAR(JSON_EXTRACT(JSON_EXTRACT(jsonPayload.request.body, '$.talk'), '$.content'), '$.message') AS talk_cust_message,
          JSON_EXTRACT_SCALAR(JSON_EXTRACT(JSON_EXTRACT(jsonPayload.request.body, '$.talk'), '$.content'), '$.value') AS talk_cust_value,
          JSON_EXTRACT_SCALAR(JSON_EXTRACT(jsonPayload.request.body, '$.object'), '$.content') AS content, --okskyからのmsg
          --response.body系をパース
          jsonPayload.response.body AS res_body,
          JSON_EXTRACT(jsonPayload.response.body, '$.messages') AS response_body_messages,
          JSON_EXTRACT_SCALAR(jsonPayload.response.body, '$.status_msg') AS response_body_status_msg
        FROM fr
        WHERE
          1=1
          AND jsonPayload.common.api = REGEXP_EXTRACT("%s", jsonPayload.common.api) --引数:extact_apisで指定したapiに限定する
      )

      --会話をパース(JSONをParseして、配列化)
      SELECT
        jp.*,
        `flightboard`.f_parse_json_response_body_messages_talk_content_message(jp.response_body_messages) AS messages, --すべてのmessageを対象
        `flightboard`.f_parse_json_response_body_messages_talk_content_chips(jp.response_body_messages) AS response_chips, --list, chip系を対象
        `flightboard`.f_parse_json_response_body_messages_talk_content_sliders(jp.response_body_messages) AS response_sliders, --slider系を対象
      FROM jp
      WHERE 
        1=1
        --AND response_body_status_msg != "Success user msg published." --C2O[POST_MESSAGE]を除外
      ORDER BY
        api,
        hmt_id,
        timestamp_jst
    );
  """, tmp_table_name, table_id_source, extract_start_date, extract_end_date, extract_api_str);

  --TEMPテーブルをRead(検証用)
  --SELECT * FROM TmpTbl_FlightRecord;
  
END;