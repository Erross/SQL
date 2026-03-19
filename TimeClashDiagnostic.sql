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