CREATE OR REPLACE PROCEDURE `flightboard.p_drop_table_flight_record_365d_client_all`()
BEGIN
/*==============================================
STORED PROCEDURE::flightboard.p_drop_table_flight_record_365d_client_all

[REMARKS]
・flightboard上に溜め込まれるflight_recordデータを格納するクライアント別テーブルを
　DROPするプロシージャーを呼ぶための親プロシージャ
　-->CliendIDは、ログのレコードから自動判定し、
　導き出された、ClientIDでループ処理を行う
　（リカバリー、クリーンアップ用）
==============================================*/

  DECLARE theDay DATE;
  --実行日を取得
  SET theDay = CURRENT_DATE("Asia/Tokyo");

--flight_recordのログレコードに登録されているすべてのClientテーブルを削除する
FOR ids IN (SELECT client FROM `gcs_logging.tf_get_flight_record_clients`(theDay, 100))
DO
  CALL `flightboard.p_drop_table_flight_record_365d_client`(ids.client);
END FOR;

END;