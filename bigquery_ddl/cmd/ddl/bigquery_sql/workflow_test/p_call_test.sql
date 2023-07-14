CREATE OR REPLACE PROCEDURE `learnlearn-208609.workflow_test.p_call_test`(arg1 STRING, arg2 STRING)
BEGIN
/*==============================================
これはテスターです
==============================================*/

  DECLARE table_id STRING;
  SET table_id = "gcs_logging.t_flight_record_import";

  SELECT arg1, arg2;

  EXECUTE IMMEDIATE format("""
      SELECT CAST(COUNT(*) AS STRING) AS countStr
      FROM `%s`
      ;
  """, table_id);
END;