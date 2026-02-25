-- =====================================================================
-- DIAGNOSTIC: TP056 and TP102 abandonment investigation
-- TP056: abandoned = '_' in SP=0 row (confirmed from mapping)
-- TP102: SP=0 is all underscores, X's must be elsewhere
-- =====================================================================

-- =====================================================================
-- SECTION A: TP056 - Confirm '_' means abandoned
-- Show ALL pee rows (all SOURCE_POSITIONs) with their ITEM_STATES
-- =====================================================================
SELECT
    'TP056'                             AS task_plan,
    pee.SOURCE_POSITION,
    pee.STATE                           AS pee_state,
    pee.ITEM_STATES,
    LENGTH(pee.ITEM_STATES)             AS len,
    pee.DATA_COLLECTION_STATE
FROM hub_owner.REQ_RUNSET runset
JOIN hub_owner.REQ_TASK rt ON rt.RUNSET_ID = runset.ID
JOIN hub_owner.PEX_PROC_EXEC pe
     ON rt.WORK_ITEM LIKE '%' || LOWER(
            SUBSTR(RAWTOHEX(pe.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe.ID),9,4)||'-'||
            SUBSTR(RAWTOHEX(pe.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe.ID),17,4)||'-'||
            SUBSTR(RAWTOHEX(pe.ID),21,12)) || '%'
JOIN hub_owner.PEX_PROC_ELEM_EXEC pee ON pee.PARENT_ID = pe.ID
WHERE runset.RUNSET_ID = 'TP056'
GROUP BY pee.SOURCE_POSITION, pee.STATE, pee.ITEM_STATES, pee.DATA_COLLECTION_STATE
ORDER BY pee.SOURCE_POSITION, pee.ITEM_STATES;


-- =====================================================================
-- SECTION B: TP102 - Show ALL pee rows (all SOURCE_POSITIONs)
-- Need to find which row contains the X's for S001053/S001056
-- =====================================================================
SELECT
    'TP102'                             AS task_plan,
    pee.SOURCE_POSITION,
    pee.STATE                           AS pee_state,
    pee.ITEM_STATES,
    LENGTH(pee.ITEM_STATES)             AS len,
    pee.DATA_COLLECTION_STATE
FROM hub_owner.REQ_RUNSET runset
JOIN hub_owner.REQ_TASK rt ON rt.RUNSET_ID = runset.ID
JOIN hub_owner.PEX_PROC_EXEC pe
     ON rt.WORK_ITEM LIKE '%' || LOWER(
            SUBSTR(RAWTOHEX(pe.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe.ID),9,4)||'-'||
            SUBSTR(RAWTOHEX(pe.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe.ID),17,4)||'-'||
            SUBSTR(RAWTOHEX(pe.ID),21,12)) || '%'
JOIN hub_owner.PEX_PROC_ELEM_EXEC pee ON pee.PARENT_ID = pe.ID
WHERE runset.RUNSET_ID = 'TP102'
GROUP BY pee.SOURCE_POSITION, pee.STATE, pee.ITEM_STATES, pee.DATA_COLLECTION_STATE
ORDER BY pee.SOURCE_POSITION, pee.ITEM_STATES;


-- =====================================================================
-- SECTION C: TP102 - Per-sample position check across all pee rows
-- Map each sample to its absolute position and show char from EACH pee row
-- =====================================================================
SELECT
    s.SAMPLE_ID,
    (SELECT COUNT(*) + 1
     FROM hub_owner.REQ_TASK rt2
     JOIN hub_owner.SAM_SAMPLE s2
          ON INSTR(','||rt2.SAMPLE_LIST||',', ','||s2.SAMPLE_ID||',') > 0
     WHERE rt2.WORK_ITEM LIKE '%' || LOWER(
                 SUBSTR(RAWTOHEX(pe.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe.ID),9,4)||'-'||
                 SUBSTR(RAWTOHEX(pe.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe.ID),17,4)||'-'||
                 SUBSTR(RAWTOHEX(pe.ID),21,12)) || '%'
       AND s2.SAMPLE_ID < s.SAMPLE_ID
    )                                   AS abs_position,
    pee.SOURCE_POSITION,
    pee.ITEM_STATES,
    UPPER(SUBSTR(pee.ITEM_STATES,
        (SELECT COUNT(*) + 1
         FROM hub_owner.REQ_TASK rt2
         JOIN hub_owner.SAM_SAMPLE s2
              ON INSTR(','||rt2.SAMPLE_LIST||',', ','||s2.SAMPLE_ID||',') > 0
         WHERE rt2.WORK_ITEM LIKE '%' || LOWER(
                     SUBSTR(RAWTOHEX(pe.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe.ID),9,4)||'-'||
                     SUBSTR(RAWTOHEX(pe.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe.ID),17,4)||'-'||
                     SUBSTR(RAWTOHEX(pe.ID),21,12)) || '%'
           AND s2.SAMPLE_ID < s.SAMPLE_ID),
        1))                             AS char_at_abs_pos,
    s.LIFE_CYCLE_STATE
FROM hub_owner.REQ_RUNSET runset
JOIN hub_owner.REQ_TASK rt ON rt.RUNSET_ID = runset.ID
JOIN hub_owner.PEX_PROC_EXEC pe
     ON rt.WORK_ITEM LIKE '%' || LOWER(
            SUBSTR(RAWTOHEX(pe.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe.ID),9,4)||'-'||
            SUBSTR(RAWTOHEX(pe.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe.ID),17,4)||'-'||
            SUBSTR(RAWTOHEX(pe.ID),21,12)) || '%'
JOIN hub_owner.PEX_PROC_ELEM_EXEC pee ON pee.PARENT_ID = pe.ID
JOIN hub_owner.SAM_SAMPLE s
     ON INSTR(','||rt.SAMPLE_LIST||',', ','||s.SAMPLE_ID||',') > 0
WHERE runset.RUNSET_ID = 'TP102'
  AND pee.ITEM_STATES IS NOT NULL
ORDER BY s.SAMPLE_ID, pee.SOURCE_POSITION;