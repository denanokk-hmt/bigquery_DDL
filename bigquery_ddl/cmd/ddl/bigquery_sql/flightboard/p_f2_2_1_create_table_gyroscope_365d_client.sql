CREATE OR REPLACE PROCEDURE `flightboard.p_f2_2_1_create_table_gyroscope_365d_client`(client STRING)
BEGIN
/*==============================================
STORED PROCEDURE::fligtboard.p_f2_2_1_create_table_gyroscope_365d_client

[ARGS]
client STRING: クライアントID

[REMARKS]
・gyroscopeからのGCSロギングデータを
　クライアント別に振り分けるためのテーブルをflightboardデータセット
　上に作成するプロシージャー
==============================================*/

  DECLARE table_id STRING;
  SET table_id = CONCAT("flightboard.t_gyroscope_365d_" , client);

  EXECUTE IMMEDIATE format("""
    CREATE TABLE IF NOT EXISTS `%s`
    (
      cdt TIMESTAMP,
      host STRING,
      url STRING,
      path STRING,
      ip STRING,
      user_agent STRING,
      referrer STRING,
      client_id STRING,
      customer_uuid STRING,
      customer_id STRING,
      hmt_id STRING,
      category_id STRING,
      type STRING,
      id STRING,
      label STRING,
      value STRING,
      wy_event STRING,
      timestamp TIMESTAMP
    )
    PARTITION BY DATE(timestamp)
    CLUSTER BY
      timestamp,
      hmt_id,
      url
    OPTIONS(
      partition_expiration_days=400,
      friendly_name="%s",
      description="storage webtracking 365day data.",
      labels=[("importance", "high"), ("confidentiality", "private")]
    );
  """, table_id, CONCAT("t_gyroscope_365d_" , client));
END;