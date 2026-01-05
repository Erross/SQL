SELECT column_name, data_type, data_length
FROM all_tab_columns
WHERE owner = 'HUB_OWNER'
  AND table_name = 'RES_MEASUREMENT'
  AND (
    UPPER(column_name) LIKE '%RAW%' OR
    UPPER(column_name) LIKE '%DATA%' OR
    UPPER(column_name) LIKE '%JSON%' OR
    UPPER(column_name) LIKE '%TEXT%' OR
    UPPER(column_name) LIKE '%RESULT%' OR
    UPPER(column_name) LIKE '%PAYLOAD%'
  )
ORDER BY column_name;

SELECT
  COUNT(*) AS matched_rows,
  SUM(CASE WHEN m.context IS NOT NULL THEN 1 ELSE 0 END) AS matched_context_not_null,
  MIN(LENGTH(m.context)) AS min_context_len,
  MAX(LENGTH(m.context)) AS max_context_len
FROM hub_owner.res_measurementsample ms
JOIN hub_owner.sam_sample s
  ON ms.sample_id = s.sample_id
JOIN hub_owner.res_measurement m
  ON m.id = ms.measurement_id;