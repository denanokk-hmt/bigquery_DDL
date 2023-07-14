CREATE OR REPLACE PROCEDURE `flightboard_contents.p_f3_3_1_get_data_op_connect_leadtime_msgs`(tmp_table_name STRING, suffix STRING)
BEGIN
/*==============================================
STORED PROCEDURE::fligtboard_contents.p_f3_3_1_get_data_op_connect_leadtime_msgs

ARGS::
・tmp_table_name：データを格納したTEMPテーブル名
・suffix："bot" or "op"

REMARKS::
・"bot"の場合
　オペレーター接続コマンドメッセージのレコードを取得し、新規作成TEMPテーブルへ格納しておく
・"op"の場合
　オペレーターメッセージのレコードを取得し、新規作成TEMPテーブルへ格納しておく
==============================================*/

  DECLARE tmp_table_bot STRING;
  DECLARE tmp_table_op STRING;

  /*--------------------------
  ■オペレーター接続コマンドメッセージの取得
  --------------------------*/
  IF suffix = "bot" THEN

    --botメッセージを格納するテンプテーブル名
    SET tmp_table_bot = CONCAT(tmp_table_name, "_bot");

    --TEMP オペレーターに接続コマンドメッセージレコードを格納
    EXECUTE IMMEDIATE format("""
      CREATE TEMP TABLE IF NOT EXISTS %s AS (
      WITH

      --post message
      post_msg AS (
        SELECT
          timestamp_jst AS timestamp_bot_jst,
          date,
          client,
          hmt_id,
          api,
          response_body_messages,
        FROM %s
        WHERE 
          1=1
          AND api IN ("POST_MESSAGE")
      ),

      --bot
      bot AS (
        WITH connect AS (
          SELECT
            timestamp_bot_jst,
            date,
            client,
            hmt_id,
            messages
          FROM 
            post_msg,
            UNNEST(`flightboard`.f_parse_json_response_body_messages_talk_content_message(post_msg.response_body_messages)) AS messages
          WHERE
            1=1
            AND messages.talk.content.message = "connect_operator"
        )
        SELECT
          date,
          MIN(timestamp_bot_jst) AS timestamp_bot_jst,
          hmt_id,
        FROM
          connect
        GROUP BY
          date,
          hmt_id
        ORDER BY
          timestamp_bot_jst,
          hmt_id
      )

      SELECT * FROM bot
    );
    """, tmp_table_bot, tmp_table_name);

  /*--------------------------
  ■オペレーターメッセージの取得
  --------------------------*/
  ELSEIF suffix = "op" THEN

    SET tmp_table_op = CONCAT(tmp_table_name, "_op");

    --TEMP OKSKYに接続後、最初のオペレーターの初回応答のレコードを格納
    EXECUTE IMMEDIATE format("""
      CREATE TEMP TABLE IF NOT EXISTS %s AS (
        WITH
          --post op recieve message
          post_op_rec_msg AS (
            SELECT
              timestamp_jst AS timestamp_op_jst,
              date,
              client,
              hmt_id,
              api,
              content,
            FROM %s
            WHERE 
              1=1
              AND api IN ("POST_OP_RECEIVE_MESSAGE")
          ),
        --op
        op AS (
          WITH op_msg AS (
            SELECT
              timestamp_op_jst,
              date,
              client,
              hmt_id,
            FROM 
              post_op_rec_msg
            WHERE
              content != "【ユーザーの接続が切断されました。】"
            ORDER BY
              hmt_id,
              timestamp_op_jst
          )
          SELECT
            date,
            hmt_id,
            MIN(timestamp_op_jst) AS timestamp_op_jst
          FROM
            op_msg
          GROUP BY
            date,
            hmt_id
          ORDER BY
            timestamp_op_jst,
            hmt_id
        )

        SELECT * FROM op
      );
    """, tmp_table_op, tmp_table_name);
  END IF;
END;