SELECT 
    m.ID as MEASUREMENT_ID,
    m.LAST_UPDATED,
    meas_s.ROW_INDEX,
    meas_s.SAMPLE_ID,
    s.SAMPLE_ID as SAM_SAMPLE_ID
FROM hub_owner.PEX_PROC_EXEC pe
JOIN hub_owner.PEX_PROC_ELEM_EXEC pee ON pee.PARENT_ID = pe.ID
JOIN hub_owner.RES_RETRIEVAL_CONTEXT ctx
     ON ctx.CONTEXT = 'urn:pexelement:' ||
        LOWER(SUBSTR(RAWTOHEX(pee.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pee.ID),9,4)||'-'||
              SUBSTR(RAWTOHEX(pee.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pee.ID),17,4)||'-'||
              SUBSTR(RAWTOHEX(pee.ID),21,12))
JOIN hub_owner.RES_MEASUREMENTSAMPLE meas_s ON meas_s.CONTEXT_ID = ctx.ID
JOIN hub_owner.RES_MEASUREMENT m ON m.ID = meas_s.MEASUREMENT_ID
JOIN hub_owner.SAM_SAMPLE s ON s.ID = meas_s.MAPPED_SAMPLE_ID
WHERE s.SAMPLE_ID IN ('S001035','S001033')
ORDER BY m.LAST_UPDATED, m.ID, meas_s.ROW_INDEX;

SELECT 
    s.SAMPLE_ID,
    meas_s.ROW_INDEX,
    meas_s.CONTEXT_ID,
    m.ID as MEASUREMENT_ID,
    m.LAST_UPDATED,
    pee.ID as PEE_ID,
    peep.ID as PEEP_ID,
    peep.SOURCE_POSITION,
    pv.ITEM_INDEX,
    pv.VALUE_NUMERIC,
    pv.VALUE_STRING
FROM hub_owner.SAM_SAMPLE s
JOIN hub_owner.RES_MEASUREMENTSAMPLE meas_s ON meas_s.MAPPED_SAMPLE_ID = s.ID
JOIN hub_owner.RES_MEASUREMENT m ON m.ID = meas_s.MEASUREMENT_ID
JOIN hub_owner.RES_RETRIEVAL_CONTEXT ctx ON ctx.ID = meas_s.CONTEXT_ID
JOIN hub_owner.PEX_PROC_ELEM_EXEC pee 
     ON ctx.CONTEXT = 'urn:pexelement:' ||
        LOWER(SUBSTR(RAWTOHEX(pee.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pee.ID),9,4)||'-'||
              SUBSTR(RAWTOHEX(pee.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pee.ID),17,4)||'-'||
              SUBSTR(RAWTOHEX(pee.ID),21,12))
JOIN hub_owner.PEX_PROC_ELEM_EXEC_PARAM peep ON peep.PARENT_ID = pee.ID
JOIN hub_owner.COR_PARAMETER_VALUE pv ON pv.PARENT_IDENTITY = peep.ID
WHERE s.SAMPLE_ID IN ('S001033','S001035','S003313')
  AND pv.VALUE_NUMERIC IS NOT NULL
ORDER BY s.SAMPLE_ID, peep.SOURCE_POSITION, pv.ITEM_INDEX;

SELECT 
    ctx.ID as CONTEXT_ID,
    COUNT(*) as MEAS_SAMPLE_COUNT
FROM hub_owner.RES_MEASUREMENTSAMPLE meas_s
JOIN hub_owner.RES_RETRIEVAL_CONTEXT ctx ON ctx.ID = meas_s.CONTEXT_ID
JOIN hub_owner.SAM_SAMPLE s ON s.ID = meas_s.MAPPED_SAMPLE_ID
WHERE s.SAMPLE_ID IN ('S001033','S001035','S003313')
GROUP BY ctx.ID;--

SELECT 
    s.SAMPLE_ID,
    meas_s.ROW_INDEX,
    MIN(ms2.ROW_INDEX) as MIN_ROW_INDEX_IN_MEASUREMENT,
    meas_s.ROW_INDEX - MIN(ms2.ROW_INDEX) as CALCULATED_LOCAL_INDEX
FROM hub_owner.SAM_SAMPLE s
JOIN hub_owner.RES_MEASUREMENTSAMPLE meas_s ON meas_s.MAPPED_SAMPLE_ID = s.ID
JOIN hub_owner.RES_MEASUREMENT m ON m.ID = meas_s.MEASUREMENT_ID
JOIN hub_owner.RES_MEASUREMENTSAMPLE ms2 ON ms2.MEASUREMENT_ID = m.ID
WHERE s.SAMPLE_ID IN ('S001029','S001030','S001031','S001032','S001033',
                       'S001035','S001036','S001037','S001038','S001039',
                       'S003313')
GROUP BY s.SAMPLE_ID, meas_s.ROW_INDEX
ORDER BY s.SAMPLE_ID;

SELECT 
    meas_s.ROW_INDEX,
    meas_s.SAMPLE_ID,
    s.SAMPLE_ID as SAM_SAMPLE_ID
FROM hub_owner.RES_MEASUREMENTSAMPLE meas_s
LEFT JOIN hub_owner.SAM_SAMPLE s ON s.ID = meas_s.MAPPED_SAMPLE_ID
WHERE meas_s.CONTEXT_ID = 'A3DB375F51B64A01B7F681B29C96AD00'
ORDER BY meas_s.ROW_INDEX;

SELECT 
    meas_s.ROW_INDEX,
    meas_s.SAMPLE_ID,
    s.SAMPLE_ID as SAM_SAMPLE_ID
FROM hub_owner.RES_MEASUREMENTSAMPLE meas_s
LEFT JOIN hub_owner.SAM_SAMPLE s ON s.ID = meas_s.MAPPED_SAMPLE_ID
WHERE meas_s.CONTEXT_ID = 'D8DDB6DB73884DC094BE7F53CE58A963'
ORDER BY meas_s.ROW_INDEX;

SELECT 
    s.SAMPLE_ID,
    meas_s.ID as MEAS_S_ID,
    meas_s.CONTEXT_ID,
    meas_s.MEASUREMENT_ID,
    meas_s.ROW_INDEX
FROM hub_owner.SAM_SAMPLE s
JOIN hub_owner.RES_MEASUREMENTSAMPLE meas_s ON meas_s.MAPPED_SAMPLE_ID = s.ID
WHERE s.SAMPLE_ID IN ('S003025','S002814','S003345','S002830','S003342',
                       'S002943','S003329','S002833','S002959','S002940',
                       'S003326','S002817','S002956')
ORDER BY s.SAMPLE_ID;

-- Step 2: Can we reach PEE via the context URN?
SELECT 
    s.SAMPLE_ID,
    meas_s.ROW_INDEX,
    ctx.CONTEXT,
    pee.ID as PEE_ID
FROM hub_owner.SAM_SAMPLE s
JOIN hub_owner.RES_MEASUREMENTSAMPLE meas_s ON meas_s.MAPPED_SAMPLE_ID = s.ID
JOIN hub_owner.RES_RETRIEVAL_CONTEXT ctx ON ctx.ID = meas_s.CONTEXT_ID
LEFT JOIN hub_owner.PEX_PROC_ELEM_EXEC pee 
     ON ctx.CONTEXT = 'urn:pexelement:' ||
        LOWER(SUBSTR(RAWTOHEX(pee.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pee.ID),9,4)||'-'||
              SUBSTR(RAWTOHEX(pee.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pee.ID),17,4)||'-'||
              SUBSTR(RAWTOHEX(pee.ID),21,12))
WHERE s.SAMPLE_ID IN ('S003025','S002814','S003345')
ORDER BY s.SAMPLE_ID;

--find nulls
SELECT 
    s.SAMPLE_ID,
    meas_s.ROW_INDEX,
    pee.ID as PEE_ID,
    peep.ID as PEEP_ID,
    peep.SOURCE_POSITION,
    (meas_s.ROW_INDEX - (
        SELECT MIN(ms2.ROW_INDEX)
        FROM hub_owner.RES_MEASUREMENTSAMPLE ms2
        WHERE ms2.CONTEXT_ID = meas_s.CONTEXT_ID
    )) as CALC_ITEM_INDEX,
    pv.ITEM_INDEX as ACTUAL_ITEM_INDEX,
    pv.VALUE_NUMERIC,
    rt.TASK_ID,
    rt.LIFE_CYCLE_STATE as TASK_STATE,
    ms_master.SAMPLE_ID as MASTER_SAMPLE_ID,
    coi.COLLABORATIVE_SPACE_ID as CS_ID
FROM hub_owner.SAM_SAMPLE s
JOIN hub_owner.RES_MEASUREMENTSAMPLE meas_s ON meas_s.MAPPED_SAMPLE_ID = s.ID
JOIN hub_owner.RES_RETRIEVAL_CONTEXT ctx ON ctx.ID = meas_s.CONTEXT_ID
JOIN hub_owner.PEX_PROC_ELEM_EXEC pee 
     ON ctx.CONTEXT = 'urn:pexelement:' ||
        LOWER(SUBSTR(RAWTOHEX(pee.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pee.ID),9,4)||'-'||
              SUBSTR(RAWTOHEX(pee.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pee.ID),17,4)||'-'||
              SUBSTR(RAWTOHEX(pee.ID),21,12))
JOIN hub_owner.PEX_PROC_EXEC pe ON pe.ID = pee.PARENT_ID
LEFT JOIN hub_owner.PEX_PROC_ELEM_EXEC_PARAM peep ON peep.PARENT_ID = pee.ID
LEFT JOIN hub_owner.COR_PARAMETER_VALUE pv 
     ON pv.PARENT_IDENTITY = peep.ID
    AND pv.VALUE_NUMERIC IS NOT NULL
    AND pv.ITEM_INDEX = (meas_s.ROW_INDEX - (
        SELECT MIN(ms2.ROW_INDEX)
        FROM hub_owner.RES_MEASUREMENTSAMPLE ms2
        WHERE ms2.CONTEXT_ID = meas_s.CONTEXT_ID
    ))
LEFT JOIN hub_owner.REQ_TASK rt
     ON rt.WORK_ITEM LIKE '%' || LOWER(
            SUBSTR(RAWTOHEX(pe.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe.ID),9,4)||'-'||
            SUBSTR(RAWTOHEX(pe.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe.ID),17,4)||'-'||
            SUBSTR(RAWTOHEX(pe.ID),21,12)) || '%'
    AND INSTR(','||rt.SAMPLE_LIST||',', ','||s.SAMPLE_ID||',') > 0
LEFT JOIN hub_owner.SAM_SAMPLE ms_master ON s.MASTER_SAMPLE_ID = ms_master.ID
LEFT JOIN hub_owner.COSPC_OBJECT_IDENTITY coi ON coi.OBJECT_ID = s.ID
WHERE s.SAMPLE_ID IN ('S002814','S003025','S003345')
ORDER BY s.SAMPLE_ID, peep.SOURCE_POSITION;

--but why

SELECT 
    s.SAMPLE_ID,
    meas_s.ROW_INDEX,
    (meas_s.ROW_INDEX - (
        SELECT MIN(ms2.ROW_INDEX)
        FROM hub_owner.RES_MEASUREMENTSAMPLE ms2
        WHERE ms2.CONTEXT_ID = meas_s.CONTEXT_ID
    )) as CALC_ITEM_INDEX,
    peep.SOURCE_POSITION,
    pv.ITEM_INDEX,
    pv.VALUE_NUMERIC,
    pv.VALUE_STRING
FROM hub_owner.SAM_SAMPLE s
JOIN hub_owner.RES_MEASUREMENTSAMPLE meas_s ON meas_s.MAPPED_SAMPLE_ID = s.ID
JOIN hub_owner.RES_RETRIEVAL_CONTEXT ctx ON ctx.ID = meas_s.CONTEXT_ID
JOIN hub_owner.PEX_PROC_ELEM_EXEC pee 
     ON ctx.CONTEXT = 'urn:pexelement:' ||
        LOWER(SUBSTR(RAWTOHEX(pee.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pee.ID),9,4)||'-'||
              SUBSTR(RAWTOHEX(pee.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pee.ID),17,4)||'-'||
              SUBSTR(RAWTOHEX(pee.ID),21,12))
JOIN hub_owner.PEX_PROC_ELEM_EXEC_PARAM peep ON peep.PARENT_ID = pee.ID
JOIN hub_owner.COR_PARAMETER_VALUE pv ON pv.PARENT_IDENTITY = peep.ID
WHERE s.SAMPLE_ID = 'S002814'
  AND peep.SOURCE_POSITION = 4
ORDER BY pv.ITEM_INDEX;

--fix?

SELECT 
    s.SAMPLE_ID,
    meas_s.ROW_INDEX,
    meas_s.MEASUREMENT_ID,
    (SELECT MIN(ms2.ROW_INDEX)
     FROM hub_owner.RES_MEASUREMENTSAMPLE ms2
     WHERE ms2.MEASUREMENT_ID = meas_s.MEASUREMENT_ID
    ) as MIN_ROW_PER_MEASUREMENT,
    meas_s.ROW_INDEX - (
        SELECT MIN(ms2.ROW_INDEX)
        FROM hub_owner.RES_MEASUREMENTSAMPLE ms2
        WHERE ms2.MEASUREMENT_ID = meas_s.MEASUREMENT_ID
    ) as CALC_ITEM_INDEX_NEW
FROM hub_owner.SAM_SAMPLE s
JOIN hub_owner.RES_MEASUREMENTSAMPLE meas_s ON meas_s.MAPPED_SAMPLE_ID = s.ID
WHERE s.SAMPLE_ID IN ('S002814','S003025','S003345',
                       'S001033','S001035')
ORDER BY s.SAMPLE_ID;

--count

SELECT 
    s.SAMPLE_ID,
    meas_s.ROW_INDEX,
    (SELECT COUNT(*)
     FROM hub_owner.RES_MEASUREMENTSAMPLE ms2
     WHERE ms2.CONTEXT_ID = meas_s.CONTEXT_ID
       AND ms2.ROW_INDEX < meas_s.ROW_INDEX
    ) as CALC_ITEM_INDEX_COUNT,
    meas_s.ROW_INDEX - (
        SELECT MIN(ms2.ROW_INDEX)
        FROM hub_owner.RES_MEASUREMENTSAMPLE ms2
        WHERE ms2.CONTEXT_ID = meas_s.CONTEXT_ID
    ) as CALC_ITEM_INDEX_MIN
FROM hub_owner.SAM_SAMPLE s
JOIN hub_owner.RES_MEASUREMENTSAMPLE meas_s ON meas_s.MAPPED_SAMPLE_ID = s.ID
WHERE s.SAMPLE_ID IN ('S002814','S003025','S003345',
                       'S001033','S001035')
ORDER BY s.SAMPLE_ID;

--ope

SELECT 
    s.SAMPLE_ID,
    meas_s.ROW_INDEX,
    meas_s.CONTEXT_ID,
    meas_s.MEASUREMENT_ID,
    (SELECT COUNT(*) FROM hub_owner.RES_MEASUREMENTSAMPLE ms2 
     WHERE ms2.CONTEXT_ID = meas_s.CONTEXT_ID) as SAMPLES_IN_CONTEXT,
    (SELECT COUNT(*) FROM hub_owner.RES_MEASUREMENTSAMPLE ms2 
     WHERE ms2.MEASUREMENT_ID = meas_s.MEASUREMENT_ID) as SAMPLES_IN_MEASUREMENT,
    (SELECT MIN(ms2.ROW_INDEX) FROM hub_owner.RES_MEASUREMENTSAMPLE ms2 
     WHERE ms2.CONTEXT_ID = meas_s.CONTEXT_ID) as MIN_ROW_CTX,
    (SELECT COUNT(*) FROM hub_owner.RES_MEASUREMENTSAMPLE ms2 
     WHERE ms2.CONTEXT_ID = meas_s.CONTEXT_ID
       AND ms2.ROW_INDEX < meas_s.ROW_INDEX) as COUNT_PRIOR_CTX,
    meas_s.ROW_INDEX - (SELECT MIN(ms2.ROW_INDEX) FROM hub_owner.RES_MEASUREMENTSAMPLE ms2 
     WHERE ms2.CONTEXT_ID = meas_s.CONTEXT_ID) as MINUS_MIN_CTX
FROM hub_owner.SAM_SAMPLE s
JOIN hub_owner.RES_MEASUREMENTSAMPLE meas_s ON meas_s.MAPPED_SAMPLE_ID = s.ID
WHERE s.SAMPLE_ID IN (
    'S001033','S001035',
    'S002814','S003025',
    'S002236'
)
ORDER BY s.SAMPLE_ID;

--universal?

SELECT 
    s.SAMPLE_ID,
    meas_s.ROW_INDEX,
    (SELECT COUNT(*)
     FROM hub_owner.RES_MEASUREMENTSAMPLE ms2
     WHERE ms2.CONTEXT_ID = meas_s.CONTEXT_ID
       AND ms2.MEASUREMENT_ID = meas_s.MEASUREMENT_ID
       AND ms2.ROW_INDEX < meas_s.ROW_INDEX
    ) as CALC_CTX_AND_MEAS
FROM hub_owner.SAM_SAMPLE s
JOIN hub_owner.RES_MEASUREMENTSAMPLE meas_s ON meas_s.MAPPED_SAMPLE_ID = s.ID
WHERE s.SAMPLE_ID IN (
    'S001033','S001035',
    'S002814','S003025',
    'S002236'
)
ORDER BY s.SAMPLE_ID;

SELECT 
    ms2.ROW_INDEX,
    ms2.MEASUREMENT_ID,
    s2.SAMPLE_ID,
    ms2.CONTEXT_ID
FROM hub_owner.RES_MEASUREMENTSAMPLE ms2
JOIN hub_owner.SAM_SAMPLE s2 ON s2.ID = ms2.MAPPED_SAMPLE_ID
WHERE ms2.CONTEXT_ID = (
    SELECT meas_s.CONTEXT_ID 
    FROM hub_owner.RES_MEASUREMENTSAMPLE meas_s
    JOIN hub_owner.SAM_SAMPLE s ON s.ID = meas_s.MAPPED_SAMPLE_ID
    WHERE s.SAMPLE_ID = 'S002236'
    AND ROWNUM = 1
)
ORDER BY ms2.MEASUREMENT_ID, ms2.ROW_INDEX;