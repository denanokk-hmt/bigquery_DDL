CREATE OR REPLACE FUNCTION flightboard.f_parse_json_response_body_messages_talk_content_sliders(s STRING) RETURNS ARRAY<STRUCT<talk STRUCT<type STRING, content STRUCT<sliders ARRAY<STRUCT<item_name STRING, item_value STRING, link STRING, img_url STRING>>>>>> LANGUAGE js
OPTIONS(
  description="注意:スキーマにネストレベルが 15 を超える RECORD タイプを含めることはできません。(https://cloud.google.com/bigquery/docs/nested-repeated)flightboard.t_flight_record_365d_[client]テーブルへ取り込まれたログから、jsonPayload.response.body.messasges.talk.content.slidersが持つ配列を取得(parse)する")
AS
r"""
return JSON.parse(s);
""";