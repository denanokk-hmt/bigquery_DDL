CREATE OR REPLACE PROCEDURE `flightboard.p_drop_table_gyroscope_365d_client`(client STRING)
BEGIN
/*==============================================
STORED PROCEDURE::flightboard.p_drop_table_gyroscope_365d_client

[ARGS]
client STRING: クライアントID

[REMARKS]
・flightboard上に溜め込まれるgyroscopeデータを格納するクライアント別テーブルを
　DROPするプロシージャー
　（リカバリー、クリーンアップ用）
==============================================*/

  DECLARE table_id_destiny STRING;

  --削除テーブルを指定
  SET table_id_destiny = CONCAT("flightboard.t_gyroscope_365d_" , client);
  
  --指定されたテーブルを削除する
  EXECUTE IMMEDIATE format("""
  DROP TABLE `%s`
  """, table_id_destiny);

END;