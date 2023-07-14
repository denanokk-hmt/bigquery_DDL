CREATE OR REPLACE PROCEDURE `flightboard_contents.p_f3_0_0_batch_exec_some_content_agg_clients_1m`(
  content STRING, 
  extract_month STRING
)
BEGIN
/*==============================================
STORED PROCEDURE::flightboard_contents.p_f3_0_0_batch_exec_some_content_agg_clients_1m

ARGS::
・content：抽出・集計したいコンテントを指定
・extract_month：抽出・集計月　※指定しない場合、実行月の前月が設定される

REMARKS::
・contentで予定されているもの（拡張あり）
  events_result
  op_connect_leadtime
  cust_msg
・p_events_result_agg_1mプロシージャーを全Client数分実行させる親プロシージャ
・全CliendIDは、ログのレコードから取得する。
　（バッチ実行用）
==============================================*/

  DECLARE theMonthBefore STRING;
  DECLARE clients ARRAY<STRING>;

  --実行日の前月を取得
  SET theMonthBefore = REGEXP_REPLACE(CAST(DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 MONTH) AS STRING), "-[0-9]*$", "");

  --抽出月を決定(抽出月に指定がない[=Monthlyバッチ運用]場合、前月を設定する)
  IF extract_month IS NULL OR extract_month = '' THEN 
    SET extract_month = theMonthBefore;
  END IF;

  --実行日から30日前までのログデータから存在しているClientを取得する(過去データのマイグレの時には、抽出で空振りさせる想定)
  CALL `flightboard_contents.p_f3_1_0_get_some_content_client_ids`(content, CAST(extract_start_date AS DATE), 30, clients);

  --Client毎に指定期間のデータの集計を行う
  FOR ids IN (SELECT client from UNNEST(clients) AS client)
  DO
    CALL `flightboard_contents.p_f3_1_0_replace_data_agg_some_content_client_1m`(content, ids.client, extract_month);
  END FOR;

END;