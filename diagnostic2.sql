-- Diagnostic 2: Find what ITEM_INDEX holds 150.0099 for S000489,
-- and what REGEXP_COUNT computes as its position in SAMPLE_LIST.

-- PART A: All pv rows for the ov_meter_reading peep in this PE,
-- regardless of ITEM_INDEX — shows every reading stored and at which index.
SELECT
    'PART_A_ALL_PV_ROWS'        AS section,
    pv.ITEM_INDEX,
    pv.VALUE_NUMERIC,
    pv.VALUE_STRING,
    peep.SOURCE_POSITION,
    RAWTOHEX(peep.ID)           AS peep_id
FROM hub_owner.PEX_PROC_EXEC pe
JOIN hub_owner.PEX_PROC_ELEM_EXEC pee
     ON pee.PARENT_ID = pe.ID
JOIN hub_owner.PEX_PROC_ELEM_EXEC_PARAM peep
     ON peep.PARENT_ID = pee.ID
JOIN hub_owner.COR_PARAMETER_VALUE pv
     ON pv.PARENT_IDENTITY = peep.ID
JOIN hub_owner.REQ_TASK rt
     ON rt.WORK_ITEM LIKE '%' || LOWER(
            SUBSTR(RAWTOHEX(pe.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe.ID),9,4)||'-'||
            SUBSTR(RAWTOHEX(pe.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe.ID),17,4)||'-'||
            SUBSTR(RAWTOHEX(pe.ID),21,12)) || '%'
    AND rt.RUNSET_ID = (SELECT ID FROM hub_owner.REQ_RUNSET WHERE RUNSET_ID = 'TP047')
WHERE pv.VALUE_NUMERIC IS NOT NULL
  AND peep.SOURCE_POSITION = 4  -- ov_meter_reading position
ORDER BY pv.ITEM_INDEX

UNION ALL

-- PART B: What REGEXP_COUNT computes as the 0-based position for S000483 and S000489
-- in rt.SAMPLE_LIST — this is what pv.ITEM_INDEX should equal after the fix.
SELECT
    'PART_B_SAMPLE_POSITION'    AS section,
    REGEXP_COUNT(
        SUBSTR(','||rt.SAMPLE_LIST, 1,
               INSTR(','||rt.SAMPLE_LIST, ','||s.SAMPLE_ID)),
        ','
    ) - 1                       AS item_index,
    NULL                        AS value_numeric,
    s.SAMPLE_ID                 AS value_string,
    NULL                        AS source_position,
    RAWTOHEX(pe.ID)             AS peep_id
FROM hub_owner.PEX_PROC_EXEC pe
JOIN hub_owner.REQ_TASK rt
     ON rt.WORK_ITEM LIKE '%' || LOWER(
            SUBSTR(RAWTOHEX(pe.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe.ID),9,4)||'-'||
            SUBSTR(RAWTOHEX(pe.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe.ID),17,4)||'-'||
            SUBSTR(RAWTOHEX(pe.ID),21,12)) || '%'
    AND rt.RUNSET_ID = (SELECT ID FROM hub_owner.REQ_RUNSET WHERE RUNSET_ID = 'TP047')
JOIN hub_owner.SAM_SAMPLE s
     ON INSTR(','||rt.SAMPLE_LIST||',', ','||s.SAMPLE_ID||',') > 0
    AND s.SAMPLE_ID IN ('S000483', 'S000489')
ORDER BY 2;