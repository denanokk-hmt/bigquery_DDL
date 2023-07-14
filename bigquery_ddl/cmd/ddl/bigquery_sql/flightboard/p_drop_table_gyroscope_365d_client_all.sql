CREATE OR REPLACE PROCEDURE `flightboard.p_drop_table_gyroscope_365d_client_all`()
BEGIN
/*==============================================
STORED PROCEDURE::flightboard.p_drop_table_gyroscope_365d_client_all

[REMARKS]
・flightboard上に溜め込まれるgyroscopeデータを格納するクライアント別テーブルを
　DROPするプロシージャーを呼ぶための親プロシージャ
　-->CliendIDは、ログのレコードから自動判定し、
　導き出された、ClientIDでループ処理を行う
　（リカバリー、クリーンアップ用）
==============================================*/

--conversion_masterに登録されているClient別にデータをExtractを行う
FOR ids IN (SELECT client FROM `gcs_logging.tf_get_gyroscope_clients`(100))
DO
  --SELECT client, extract_date;
  CALL `flightboard.p_drop_table_gyroscope_365d_client`(ids.client);
END FOR;

END;