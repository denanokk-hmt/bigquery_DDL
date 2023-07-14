CREATE OR REPLACE PROCEDURE `flightboard.p_f2_1_1_create_table_flight_record_365d_client`(client STRING)
BEGIN
/*==============================================
STORED PROCEDURE::flightboard.p_f2_1_1_create_table_flight_record_365d_client

・flight_recorderからのGCSロギングデータを
　クライアント別に振り分けるためのテーブルをflightboardデータセット
　上に作成するプロシージャー
==============================================*/

  DECLARE table_id STRING;
  SET table_id = CONCAT("flightboard.t_flight_record_365d_" , client);

EXECUTE IMMEDIATE format("""
CREATE TABLE IF NOT EXISTS `%s`
(
Cdt TIMESTAMP,
jsonPayload	
  STRUCT<	
    common
      STRUCT<
        hmt_id	STRING,	
        type	STRING,	
        logid	STRING,			
        api	STRING,		
        client	STRING,			
        service	STRING,			
        component	STRING,			
        customer_uuid	STRING,
        use	STRING,
        session_id	STRING >,
    response
      STRUCT<
        body	STRING,	
        headers	STRING,			
        responded	STRING >,
    request
      STRUCT<
        body	STRING,		
        headers	STRING >,
    session
      STRUCT<
        token	STRING,		
        uid	STRING,		
        rid	STRING,			
        op_session	STRING,
        op_rid	STRING,	
        op_access_token	STRING,
        op_cust_uid	STRING,	
        op_system	STRING,		
        op_ope_uid	STRING >,
    kind	STRING,
    customer_uuid	STRING,
    logid	STRING
  >,
  timestamp	TIMESTAMP
)
PARTITION BY DATE(timestamp)
OPTIONS
  (
    friendly_name = "`%s`"
    , description = "extract from flight_recorder logging data."
    , partition_expiration_days=400
    , labels = [("importance", "high"), ("confidentiality","private")]
  )
""", table_id, CONCAT("t_flight_record_365d_" , client));
END;