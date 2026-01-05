SELECT
  COUNT(*) AS matched_rows,
  SUM(CASE WHEN m.raw_data IS NOT NULL THEN 1 ELSE 0 END) AS matched_raw_data_not_null,
  SUM(CASE WHEN m.raw_data_long_text IS NOT NULL THEN 1 ELSE 0 END) AS matched_raw_data_long_not_null
FROM hub_owner.res_measurementsample ms
JOIN hub_owner.sam_sample s
  ON ms.sample_id = s.sample_id
JOIN hub_owner.res_measurement m
  ON m.id = ms.measurement_id;