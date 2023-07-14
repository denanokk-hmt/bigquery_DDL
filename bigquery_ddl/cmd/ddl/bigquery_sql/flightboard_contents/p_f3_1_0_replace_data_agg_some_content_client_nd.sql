CREATE OR REPLACE PROCEDURE `flightboard_contents.p_f3_1_0_replace_data_agg_some_content_client_nd`(
  content STRING, 
  client STRING, 
  extract_start_date DATE, 
  extract_end_date DATE, 
  period INT64, 
  EXAM BOOL
)
BEGIN
/*==============================================
STORED PROCEDURE::fligtboard_contents.p_f3_1_0_replace_data_agg_some_content_client_nd

ARGS::
・content：抽出・集計したいコンテントを指定
・client：抽出・集計クライアント
・extract_start_date：抽出・集計開始日
・extract_end_date：抽出・集計終了日
・period：集計期間(範囲)
・EXAM：TRUEの場合、extarctされたデータを取得して終了する

REMARKS
・contentに応じたを集計する
・Daily、1~30日ベースの集計
　例):「2022-01-01から2022-03-31までの期間に、Daily集計をしたい」
    extract_start_date::"2022-01-01"
    extract_end_date::"2022-03-31"
    period::1
・Percentailでの集計方法
・集計結果は、t_op_connect_leadtime_30dに挿入される
==============================================*/

  DECLARE table_id STRING;
  DECLARE tmp_table STRING;
  DECLARE z INT64 DEFAULT 0;
  DECLARE x INT64 DEFAULT 0;
  DECLARE loop INT64 DEFAULT 0;
  DECLARE extract_mod_days INT64 DEFAULT 0;

  /*-----------------------------
  ■集計事前準備（箱、データ）
  -----------------------------*/

  --集計テーブルを作成
  CALL `flightboard_contents.p_f3_2_0_create_table_some_content_agg`(content, "30d");

  --ログデータ(gyroscope or flight_recordの_365d_[client])から指定期間のデータを抽出し、TEMPテーブルへ収納する
  CALL `flightboard_contents.p_f3_3_0_create_tmp_table_some_content_client_nd`(content, client, extract_start_date, extract_end_date, tmp_table);

  --レコードが取得出来なかった場合(tmp_tableがNullになる)、処理を中断
  IF tmp_table IS NULL THEN
    SELECT "no record.";
    RETURN;
  END IF;

  /*-----------------------------
  ■検証用
  -----------------------------*/
  IF EXAM = TRUE THEN
    EXECUTE IMMEDIATE format("""  SELECT * FROM %s; """, tmp_table);
    RETURN;
  END IF;

  /*--------------------------
  ■LOOP処理計算準備
  --------------------------*/

  --日付の期間日数を計算(LOOP回数)
  SET z = DATE_DIFF(extract_end_date, extract_start_date, DAY) + 1;

  --LOOP数
  SET loop = CAST(CEIL(SAFE_DIVIDE(z, period)) AS INT64);
  
  --剰余日数
  SET extract_mod_days = MOD(z, period);

  --剰余日数を加算
  SET z = z + extract_mod_days;

  /*-----------------------------
  ■period期間毎の集計を、extract_start_date~extract_end_dateまで行う
  -----------------------------*/
  BEGIN TRANSACTION;
  LOOP

    --開始日
    IF x > 0 THEN    
      SET extract_start_date = DATE_ADD(extract_start_date, INTERVAL period DAY);
    END IF;

    --終了日
    IF period = 1 THEN
      SET extract_end_date = extract_start_date;
    ELSE 
      IF x < loop OR extract_mod_days = 0 THEN
        SET extract_end_date = DATE_ADD(extract_start_date, INTERVAL period-1 DAY);
      ELSE
        SET extract_end_date = DATE_ADD(extract_start_date, INTERVAL extract_mod_days-1 DAY);
      END IF;
    END IF;

    --ループカウンタ
    SET x = x + period ;
    IF x > z THEN
      LEAVE;
    END IF;

    --集計データを指定期間でDELETE & INSERT
    CALL `flightboard_contents.p_f3_4_0_sql_execute_agg_some_content`(content, tmp_table, client, extract_start_date, extract_end_date, period, "30d");

  END LOOP;
  COMMIT TRANSACTION;

  EXCEPTION WHEN ERROR THEN
    ROLLBACK TRANSACTION;
    SELECT
      @@error.message,
      @@error.stack_trace,
      @@error.statement_text,
      @@error.formatted_stack_trace;

END;