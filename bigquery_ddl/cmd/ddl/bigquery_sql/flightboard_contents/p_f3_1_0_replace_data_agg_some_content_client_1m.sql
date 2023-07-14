CREATE OR REPLACE PROCEDURE `flightboard_contents.p_f3_1_0_replace_data_agg_some_content_client_1m`(content STRING, client STRING, extract_month STRING)
BEGIN
/*==============================================
STORED PROCEDURE::fligtboard_contents.p_f3_1_0_replace_data_agg_some_content_client_1m

ARGS::
・content：抽出・集計したいコンテントを指定
・client[string]:クライアントコード
・extract_month[string]:集計したい年月(yyyy--mm)

REMARKS::
・gyroscope or flight_recordのMonthly集計
・1ヶ月分のみのMonthly集計
・Daily(period=1)で重ねた集計を月ごとにカウントする
・集計結果は、t_events_result_12mに挿入される
==============================================*/

  DECLARE extract_month_start_date DATE;
  DECLARE extract_month_end_date DATE;

  /*--------------------------
  ■集計テーブル、集計データの準備
  --------------------------*/

  --集計テーブルを作成
  CALL `flightboard_contents.p_f3_2_0_create_table_some_content_agg`(content, "12m");

  --集計月の初日を設定(パーティション分割用カラム、_30d検索用)
  SET extract_month_start_date = CAST(CONCAT(extract_month, "-01") AS DATE);

  --集計月の最終日を設定(_30d検索用)
  SET extract_month_end_date = LAST_DAY(extract_month_start_date);

  /*--------------------------
  ■月次集計を行う
  --------------------------*/
  BEGIN TRANSACTION;

    --集計データを指定期間でDELETE & INSERT
    CALL `flightboard_contents.p_f3_4_0_sql_execute_agg_some_content`(content, "", client, extract_month_start_date, extract_month_end_date, 0, "12m");

  COMMIT TRANSACTION;

  EXCEPTION WHEN ERROR THEN
    ROLLBACK TRANSACTION;
    SELECT
      @@error.message,
      @@error.stack_trace,
      @@error.statement_text,
      @@error.formatted_stack_trace;

END;