SELECT
  JSON_TYPE(NVL(m.raw_data_long_text, TO_CLOB(m.raw_data)), '$.results') AS results_json_type,
  COUNT(*) AS row_count
FROM hub_owner.res_measurement m
GROUP BY JSON_TYPE(NVL(m.raw_data_long_text, TO_CLOB(m.raw_data)), '$.results')
ORDER BY row_count DESC;
