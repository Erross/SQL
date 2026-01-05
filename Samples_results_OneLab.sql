SELECT
  CASE
    /* If results behaves like a JSON array, this path should exist */
    WHEN JSON_EXISTS(NVL(m.raw_data_long_text, TO_CLOB(m.raw_data)), '$.results[0]')
      THEN 'ARRAY'

    /* If results is a scalar string, JSON_VALUE can extract it */
    WHEN JSON_VALUE(
           NVL(m.raw_data_long_text, TO_CLOB(m.raw_data)),
           '$.results'
           RETURNING CLOB
           NULL ON ERROR
         ) IS NOT NULL
      THEN 'STRING'

    /* results key exists but not array[0] and not scalar string */
    WHEN JSON_EXISTS(NVL(m.raw_data_long_text, TO_CLOB(m.raw_data)), '$.results')
      THEN 'OTHER_JSON_TYPE'

    ELSE 'MISSING'
  END AS results_storage_type,
  COUNT(*) AS row_count
FROM hub_owner.res_measurement m
GROUP BY
  CASE
    WHEN JSON_EXISTS(NVL(m.raw_data_long_text, TO_CLOB(m.raw_data)), '$.results[0]')
      THEN 'ARRAY'
    WHEN JSON_VALUE(
           NVL(m.raw_data_long_text, TO_CLOB(m.raw_data)),
           '$.results'
           RETURNING CLOB
           NULL ON ERROR
         ) IS NOT NULL
      THEN 'STRING'
    WHEN JSON_EXISTS(NVL(m.raw_data_long_text, TO_CLOB(m.raw_data)), '$.results')
      THEN 'OTHER_JSON_TYPE'
    ELSE 'MISSING'
  END
ORDER BY row_count DESC;
