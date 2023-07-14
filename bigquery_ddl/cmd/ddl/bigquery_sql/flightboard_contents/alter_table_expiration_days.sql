

ALTER TABLE IF EXISTS flightboard_contents.t_mau_counter_30d_222
SET OPTIONS (
  partition_expiration_days=180
)