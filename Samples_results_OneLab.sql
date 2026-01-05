SELECT
  s.name      AS sample_name,
  s.sample_id AS sample_id,

  ms.sample_id AS ms_sample_id,
  ms.measurement_id,

  m.id AS measurement_id_check,
  m.raw_data AS result_payload_short,
  DBMS_LOB.SUBSTR(m.raw_data_long_text, 4000, 1) AS result_payload_long_4k

FROM hub_owner.res_measurementsample ms
JOIN hub_owner.sam_sample s
  ON ms.sample_id = s.sample_id          -- ✅ string-to-string join
JOIN hub_owner.res_measurement m
  ON m.id = ms.measurement_id            -- RAW-to-RAW join (measurement id)

WHERE (m.raw_data IS NOT NULL OR m.raw_data_long_text IS NOT NULL)

ORDER BY
  s.name, s.sample_id, ms.measurement_id;
