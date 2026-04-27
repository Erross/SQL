-- =========================================================
-- DIAGNOSTIC: How does the UI know a sample is "abandoned"?
-- Run each section separately and share results
-- Target: TP064 samples that appear abandoned in UI but
--         show LIFE_CYCLE_STATE = 'planned' in SAM_SAMPLE
-- =========================================================

-- =========================================================
-- SECTION 1: SAM_SAMPLE_EVENT
-- Theory: The UI writes a lifecycle event when a sample is
-- abandoned within a task context. EVENT_CONTEXT may hold
-- the runset/task URN, making this per-task, not global.
-- =========================================================
SELECT
    s.SAMPLE_ID,
    e.EVENT_TYPE,
    e.LIFE_CYCLE_STATE     AS event_lc_state,
    e.EVENT_CONTEXT,
    e.EVENT_DATA,
    e.EVENT_TIME,
    e.SAMPLE_GROUP
FROM hub_owner.SAM_SAMPLE_EVENT e
JOIN hub_owner.SAM_SAMPLE s ON s.ID = e.SAMPLE_ID
JOIN hub_owner.REQ_RUNSET runset ON runset.RUNSET_ID = 'TP064'
JOIN hub_owner.REQ_TASK rt ON rt.RUNSET_ID = runset.ID
WHERE INSTR(','||rt.SAMPLE_LIST||',', ','||s.SAMPLE_ID||',') > 0
ORDER BY s.SAMPLE_ID, e.EVENT_TIME;


-- =========================================================
-- SECTION 2: AUD_SAM_SAMPLE (Hibernate Envers audit trail)
-- Theory: SAM_SAMPLE.LIFE_CYCLE_STATE WAS set to 'abandoned'
-- at some point and then reverted (or is still 'abandoned'
-- in a revision not yet reflected in the live table).
-- =========================================================
SELECT
    s.SAMPLE_ID,
    aud.LIFE_CYCLE_STATE   AS historical_lc_state,
    aud.REVTYPE,           -- 0=INSERT 1=UPDATE 2=DELETE
    rev.TIMESTAMP          AS change_time,
    rev.USERNAME
FROM hub_owner.AUD_SAM_SAMPLE aud
JOIN hub_owner.COR_AUDIT_REVISION rev ON rev.ID = aud.REV
JOIN hub_owner.SAM_SAMPLE s ON s.ID = aud.ID
JOIN hub_owner.REQ_RUNSET runset ON runset.RUNSET_ID = 'TP064'
JOIN hub_owner.REQ_TASK rt ON rt.RUNSET_ID = runset.ID
WHERE INSTR(','||rt.SAMPLE_LIST||',', ','||s.SAMPLE_ID||',') > 0
  AND aud.LIFE_CYCLE_STATE IS NOT NULL
ORDER BY s.SAMPLE_ID, rev.TIMESTAMP;


-- =========================================================
-- SECTION 3: PEX_PROC_ELEM_EXEC SOURCE_POSITION
-- Theory: pee.SOURCE_POSITION is the sample's ABSOLUTE
-- batch position (not task-relative ITEM_INDEX). If so,
-- SUBSTR(pee.ITEM_STATES, pee.SOURCE_POSITION, 1) gives
-- the correct per-sample state character.
-- =========================================================
SELECT
    s.SAMPLE_ID,
    s.LIFE_CYCLE_STATE              AS sam_lc_state,
    pv.ITEM_INDEX                   AS task_item_index,
    pee.SOURCE_POSITION             AS pee_source_position,
    pee.STATE                       AS pee_state,
    pee.ITEM_STATES,
    LENGTH(pee.ITEM_STATES)         AS item_states_len,
    REGEXP_COUNT(rt.SAMPLE_LIST, ',')+1 AS n_samples_in_task,
    -- If SOURCE_POSITION is absolute batch position:
    SUBSTR(pee.ITEM_STATES, pee.SOURCE_POSITION, 1)  AS char_at_source_pos,
    -- The periodic formula (what we've been using - BROKEN for multi-task):
    SUBSTR(pee.ITEM_STATES, pv.ITEM_INDEX + 1, 1)    AS char_at_item_index_plus1
FROM hub_owner.REQ_RUNSET runset
JOIN hub_owner.REQ_TASK rt ON rt.RUNSET_ID = runset.ID
JOIN hub_owner.COR_PARAMETER_VALUE pv
     ON pv.VALUE_KEY = 'A' AND pv.VALUE_STRING IS NOT NULL
JOIN hub_owner.COR_PARAMETER p
     ON pv.PARENT_IDENTITY = p.ID AND p.DISPLAY_NAME != 'Sample'
JOIN hub_owner.REQ_TASK_PARAMETER rtp
     ON p.ID = rtp.PARAMETER_ID AND rtp.TASK_ID = rt.ID
JOIN hub_owner.SAM_SAMPLE s
     ON s.SAMPLE_ID = REGEXP_SUBSTR(rt.SAMPLE_LIST, '[^,]+', 1, pv.ITEM_INDEX + 1)
JOIN hub_owner.PEX_PROC_EXEC pe
     ON rt.WORK_ITEM LIKE '%' || LOWER(
            SUBSTR(RAWTOHEX(pe.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe.ID),9,4)||'-'||
            SUBSTR(RAWTOHEX(pe.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe.ID),17,4)||'-'||
            SUBSTR(RAWTOHEX(pe.ID),21,12)) || '%'
JOIN hub_owner.PEX_PROC_ELEM_EXEC pee
     ON pee.PARENT_ID = pe.ID
     AND pee.ITEM_STATES IS NOT NULL
     AND pee.ITEM_STATES NOT LIKE '%\_%' ESCAPE '\'
WHERE runset.RUNSET_ID = 'TP064'
  AND pv.VALUE_KEY = 'A'
  AND s.SAMPLE_ID IS NOT NULL
GROUP BY
    s.SAMPLE_ID, s.LIFE_CYCLE_STATE, pv.ITEM_INDEX,
    pee.SOURCE_POSITION, pee.STATE, pee.ITEM_STATES, rt.SAMPLE_LIST
ORDER BY s.SAMPLE_ID, pee.SOURCE_POSITION;


-- =========================================================
-- SECTION 4: AUD_PEX_PROC_ELEM_EXEC - historical ITEM_STATES
-- Theory: ITEM_STATES in the current pee row was overwritten.
-- The audit trail may show a version where X appeared at the
-- CORRECT position (ITEM_INDEX+1) before the batch grew.
-- =========================================================
SELECT
    s.SAMPLE_ID,
    aud.ITEM_STATES,
    LENGTH(aud.ITEM_STATES)  AS len,
    aud.SOURCE_POSITION,
    aud.STATE,
    aud.REVTYPE,
    rev.TIMESTAMP            AS change_time
FROM hub_owner.AUD_PEX_PROC_ELEM_EXEC aud
JOIN hub_owner.COR_AUDIT_REVISION rev ON rev.ID = aud.REV
JOIN hub_owner.PEX_PROC_EXEC pe ON pe.ID = aud.PARENT_ID
JOIN hub_owner.REQ_TASK rt
     ON rt.WORK_ITEM LIKE '%' || LOWER(
            SUBSTR(RAWTOHEX(pe.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe.ID),9,4)||'-'||
            SUBSTR(RAWTOHEX(pe.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe.ID),17,4)||'-'||
            SUBSTR(RAWTOHEX(pe.ID),21,12)) || '%'
JOIN hub_owner.REQ_RUNSET runset ON runset.ID = rt.RUNSET_ID
JOIN hub_owner.SAM_SAMPLE s
     ON INSTR(','||rt.SAMPLE_LIST||',', ','||s.SAMPLE_ID||',') > 0
WHERE runset.RUNSET_ID = 'TP064'
  AND aud.ITEM_STATES IS NOT NULL
ORDER BY s.SAMPLE_ID, rev.TIMESTAMP;









