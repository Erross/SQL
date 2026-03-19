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