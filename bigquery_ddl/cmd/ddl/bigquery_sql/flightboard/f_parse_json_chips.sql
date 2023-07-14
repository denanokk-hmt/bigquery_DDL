CREATE OR REPLACE FUNCTION flightboard.f_parse_json_chips(s STRING) 
RETURNS ARRAY<STRUCT<item_name STRING, item_value STRING>> LANGUAGE js
OPTIONS(
  description="注意:スキーマにネストレベルが 15 を超える RECORD タイプを含めることはできません。(https://cloud.google.com/bigquery/docs/nested-repeated)flightboard.t_flight_record_365d_[client]テーブルへ取り込まれたログから、jsonPayload.response.body.messasges.talk.content.chipsが持つ配列を取得(parse)する")
AS
r"""
return JSON.parse(s);
""";