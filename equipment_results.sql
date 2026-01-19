SELECT 
    m.ID,
    m.RECORD_NAME,
    m.RAW_DATA,
    DBMS_LOB.SUBSTR(m.RAW_DATA_LONG_TEXT, 4000, 1) as RAW_DATA_PREVIEW,
    ms.SAMPLE_ID,
    m.MEASUREMENT_TYPE
FROM RES_MEASUREMENT m
JOIN RES_MEASUREMENTSAMPLE ms ON m.ID = ms.MEASUREMENT_ID
WHERE ms.SAMPLE_ID = 'S001'
AND ROWNUM <= 5;

--prior doesnt give the data, just meta data on it next query to look for the data

SELECT 
    ms.SAMPLE_ID,
    ms.ROW_INDEX,
    p.DISPLAY_LABEL,
    pv.VALUE_STRING,
    pv.VALUE_NUMERIC,
    pv.VALUE_TEXT
FROM RES_MEASUREMENTSAMPLE ms
JOIN COR_OBJECT_IDENTITY oi ON oi.OBJECT_ID = ms.ID
JOIN COR_CLASS_IDENTITY ci ON ci.ID = oi.CLASS_IDENTITY_ID
JOIN COR_PROPERTY_VALUE pv ON pv.OBJECT_IDENTITY_ID = oi.ID  
JOIN COR_PROPERTY p ON p.NAME = pv.PROPERTY_ID
WHERE ms.SAMPLE_ID = 'S001'
AND ci.TABLE_NAME = 'res_measurementsample';

--that gave nothing so lets try

SELECT 
    m.ID,
    m.MEASUREMENT_TYPE,
    m.INFO,
    m.CONTEXT,
    m.RAW_DATA,
    DBMS_LOB.SUBSTR(m.RAW_DATA_LONG_TEXT, 4000, 1) as RAW_DATA_PREVIEW
FROM RES_MEASUREMENT m
JOIN RES_MEASUREMENTSAMPLE ms ON m.ID = ms.MEASUREMENT_ID
WHERE ms.SAMPLE_ID = 'S001';

--that didn't help either, next

SELECT 
    p.DISPLAY_NAME,
    pv.VALUE_STRING,
    pv.VALUE_NUMERIC,
    pv.INTERPRETATION
FROM RES_MEASUREMENTSAMPLE ms
JOIN COR_PARAMETER_VALUE pv ON pv.PARENT_IDENTITY = ms.ID
JOIN COR_PARAMETER p ON p.ID = pv.PARAMETER_ID
WHERE ms.SAMPLE_ID = 'S001'
AND pv.VALUE_KEY = 'A';

--couple steps skipped, next big q

SELECT 
    pv.ID,
    pv.PARENT_IDENTITY,
    pv.VALUE_NUMERIC,
    pv.VALUE_STRING,
    pv.ITEM_INDEX,
    pv.VALUE_KEY,
    pv.INTERPRETATION
FROM COR_PARAMETER_VALUE pv
JOIN COR_PARAMETER p ON p.ID = pv.PARENT_IDENTITY
WHERE p.ID = '<paste that long ID string here>'
AND pv.VALUE_NUMERIC = 19.2
AND ROWNUM <= 5;

--try pex values? (process execution)

SELECT 
    ctx.CONTEXT as pex_urn,
    pex.ID,
    pex.STATE
FROM RES_RETRIEVAL_CONTEXT ctx
JOIN RES_MEASUREMENTSAMPLE ms ON ctx.ID = ms.CONTEXT_ID
JOIN PEX_PROC_ELEM_EXEC pex ON ctx.CONTEXT = 'urn:compose:pex_proc_elem_exec:' || RAWTOHEX(pex.ID)
WHERE ms.SAMPLE_ID = 'S001';

--and more

SELECT 
    pv.VALUE_NUMERIC,
    pv.VALUE_STRING,
    peep.ID as ELEM_EXEC_PARAM_ID,
    pee.ID as PROC_ELEM_EXEC_ID,
    pe.ID as PROC_EXEC_ID,
    ctx.ID as CONTEXT_ID,
    ctx.CONTEXT,
    ms.SAMPLE_ID,
    ms.MEASUREMENT_ID
FROM COR_PARAMETER_VALUE pv
JOIN PEX_PROC_ELEM_EXEC_PARAM peep ON peep.ID = pv.PARENT_IDENTITY
JOIN PEX_PROC_ELEM_EXEC pee ON pee.ID = peep.PARENT_ID
JOIN PEX_PROC_EXEC pe ON pe.ID = pee.PARENT_ID
JOIN RES_RETRIEVAL_CONTEXT ctx ON ctx.CONTEXT = 'urn:compose:pex_proc_elem_exec:' || RAWTOHEX(pee.ID)
JOIN RES_MEASUREMENTSAMPLE ms ON ms.CONTEXT_ID = ctx.ID
WHERE pv.VALUE_NUMERIC = 19.2
AND ROWNUM = 1;

SELECT 
    'urn:pexelement:' || RAWTOHEX(pee.ID) as constructed_urn,
    ctx.CONTEXT as actual_urn,
    CASE WHEN 'urn:pexelement:' || RAWTOHEX(pee.ID) = ctx.CONTEXT THEN 'MATCH' ELSE 'NO MATCH' END as match_status
FROM COR_PARAMETER_VALUE pv
JOIN PEX_PROC_ELEM_EXEC_PARAM peep ON peep.ID = pv.PARENT_IDENTITY
JOIN PEX_PROC_ELEM_EXEC pee ON pee.ID = peep.PARENT_ID
JOIN PEX_PROC_EXEC pe ON pe.ID = pee.PARENT_ID
CROSS JOIN (
    SELECT ctx.* 
    FROM RES_RETRIEVAL_CONTEXT ctx
    JOIN RES_MEASUREMENTSAMPLE ms ON ms.CONTEXT_ID = ctx.ID
    WHERE ms.SAMPLE_ID = 'S001'
) ctx
WHERE pv.VALUE_NUMERIC = 19.2
AND ROWNUM = 1;

--handle pex properly
SELECT *
FROM COR_PARAMETER_VALUE pv
JOIN PEX_PROC_ELEM_EXEC_PARAM peep ON peep.ID = pv.PARENT_IDENTITY
JOIN PEX_PROC_ELEM_EXEC pee ON pee.ID = peep.PARENT_ID
JOIN PEX_PROC_EXEC pe ON pe.ID = pee.PARENT_ID
JOIN RES_RETRIEVAL_CONTEXT ctx ON ctx.CONTEXT = 
    'urn:pexelement:' || 
    LOWER(
        SUBSTR(RAWTOHEX(pee.ID), 1, 8) || '-' ||
        SUBSTR(RAWTOHEX(pee.ID), 9, 4) || '-' ||
        SUBSTR(RAWTOHEX(pee.ID), 13, 4) || '-' ||
        SUBSTR(RAWTOHEX(pee.ID), 17, 4) || '-' ||
        SUBSTR(RAWTOHEX(pee.ID), 21, 12)
    )
WHERE pv.VALUE_NUMERIC = 19.2
AND ROWNUM = 1;

--bring pv in

SELECT *
FROM COR_PARAMETER_VALUE pv
JOIN PEX_PROC_ELEM_EXEC_PARAM peep ON peep.ID = pv.PARENT_IDENTITY
JOIN PEX_PROC_ELEM_EXEC pee ON pee.ID = peep.PARENT_ID
JOIN PEX_PROC_EXEC pe ON pe.ID = pee.PARENT_ID
JOIN RES_RETRIEVAL_CONTEXT ctx ON ctx.CONTEXT = 
    'urn:pexelement:' || 
    LOWER(
        SUBSTR(RAWTOHEX(pee.ID), 1, 8) || '-' ||
        SUBSTR(RAWTOHEX(pee.ID), 9, 4) || '-' ||
        SUBSTR(RAWTOHEX(pee.ID), 13, 4) || '-' ||
        SUBSTR(RAWTOHEX(pee.ID), 17, 4) || '-' ||
        SUBSTR(RAWTOHEX(pee.ID), 21, 12)
    )
JOIN RES_MEASUREMENTSAMPLE ms ON ms.CONTEXT_ID = ctx.ID
WHERE pv.VALUE_NUMERIC = 19.2;

SELECT *
FROM COR_PARAMETER_VALUE pv
JOIN PEX_PROC_ELEM_EXEC_PARAM peep ON peep.ID = pv.PARENT_IDENTITY
JOIN PEX_PROC_ELEM_EXEC pee ON pee.ID = peep.PARENT_ID
JOIN PEX_PROC_EXEC pe ON pe.ID = pee.PARENT_ID
JOIN RES_RETRIEVAL_CONTEXT ctx ON ctx.CONTEXT = 
    'urn:pexelement:' || 
    LOWER(
        SUBSTR(RAWTOHEX(pee.ID), 1, 8) || '-' ||
        SUBSTR(RAWTOHEX(pee.ID), 9, 4) || '-' ||
        SUBSTR(RAWTOHEX(pee.ID), 13, 4) || '-' ||
        SUBSTR(RAWTOHEX(pee.ID), 17, 4) || '-' ||
        SUBSTR(RAWTOHEX(pee.ID), 21, 12)
    )
JOIN RES_MEASUREMENTSAMPLE ms ON ms.CONTEXT_ID = ctx.ID
JOIN RES_MEASUREMENT m ON m.ID = ms.MEASUREMENT_ID
WHERE pv.VALUE_NUMERIC = 19.2;