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