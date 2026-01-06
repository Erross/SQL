-- ========================================
-- FOLLOW-UP INVESTIGATION
-- Step 2 worked, Step 3 failed
-- This means results exist but not via RES_MEASUREMENT path
-- ========================================

-- QUERY A: What does PARENT_IDENTITY point to for our results?
-- ========================================
PROMPT === QUERY A: Where is PARENT_IDENTITY pointing? ===
SELECT 
    pv.ID as PARAM_VALUE_ID,
    pv.PARENT_IDENTITY,
    pv.VALUE_NUMERIC,
    pv.VALUE_NUMERIC_TEXT,
    pv.VALUE_KEY,
    pv.VALUE_TYPE,
    pv.INTERPRETATION,
    pv.LAST_UPDATED
FROM COR_PARAMETER_VALUE pv
WHERE (
    (pv.VALUE_NUMERIC BETWEEN 41.0 AND 41.1)
    OR (pv.VALUE_NUMERIC BETWEEN 42.5 AND 42.6)
    OR pv.VALUE_NUMERIC_TEXT LIKE '41.02%'
    OR pv.VALUE_NUMERIC_TEXT LIKE '42.51%'
)
ORDER BY pv.VALUE_NUMERIC, pv.LAST_UPDATED DESC;

-- Copy the PARENT_IDENTITY values from above
-- Then try to find what table they belong to...


-- QUERY B: Is PARENT_IDENTITY pointing to COR_PARAMETER?
-- ========================================
PROMPT === QUERY B: Are results linked via COR_PARAMETER? ===
SELECT 
    'COR_PARAMETER' as PARENT_TABLE,
    pv.VALUE_NUMERIC,
    pv.VALUE_KEY,
    pv.PARENT_IDENTITY,
    p.ID as PARAMETER_ID,
    p.NAME as PARAMETER_NAME,
    p.DESCRIPTION as PARAMETER_DESC,
    p.URN as PARAMETER_URN
FROM COR_PARAMETER_VALUE pv
JOIN COR_PARAMETER p ON pv.PARENT_IDENTITY = p.ID
WHERE (
    (pv.VALUE_NUMERIC BETWEEN 41.0 AND 41.1)
    OR (pv.VALUE_NUMERIC BETWEEN 42.5 AND 42.6)
);


-- QUERY C: If linked via COR_PARAMETER, find the task
-- ========================================
PROMPT === QUERY C: Which tasks use these parameters? ===
SELECT 
    pv.VALUE_NUMERIC,
    pv.VALUE_KEY,
    p.NAME as PARAMETER_NAME,
    rtp.TASK_ID,
    rt.TASK_ID as TASK_ID_TEXT,
    rt.TASK_NAME,
    rt.METHOD_ID,
    rt.STATUS,
    rt.RUNSET_ID
FROM COR_PARAMETER_VALUE pv
JOIN COR_PARAMETER p ON pv.PARENT_IDENTITY = p.ID
JOIN REQ_TASK_PARAMETER rtp ON p.ID = rtp.PARAMETER_ID
JOIN REQ_TASK rt ON rtp.TASK_ID = rt.ID
WHERE (
    (pv.VALUE_NUMERIC BETWEEN 41.0 AND 41.1)
    OR (pv.VALUE_NUMERIC BETWEEN 42.5 AND 42.6)
)
ORDER BY pv.VALUE_NUMERIC;


-- QUERY D: Connect those tasks back to samples
-- ========================================
PROMPT === QUERY D: Which samples are in those tasks? ===
SELECT 
    s.SAMPLE_ID,
    s.NAME as SAMPLE_NAME,
    rt.TASK_ID as TASK_ID_TEXT,
    rt.TASK_NAME,
    rt.METHOD_ID,
    pv.VALUE_NUMERIC,
    pv.VALUE_KEY,
    p.NAME as PARAMETER_NAME
FROM COR_PARAMETER_VALUE pv
JOIN COR_PARAMETER p ON pv.PARENT_IDENTITY = p.ID
JOIN REQ_TASK_PARAMETER rtp ON p.ID = rtp.PARAMETER_ID
JOIN REQ_TASK rt ON rtp.TASK_ID = rt.ID
JOIN REQ_RUNSET_SAMPLE rs ON rt.RUNSET_ID = rs.RUNSET_ID
JOIN SAM_SAMPLE s ON rs.SAMPLE_ID = s.ID
WHERE (
    (pv.VALUE_NUMERIC BETWEEN 41.0 AND 41.1)
    OR (pv.VALUE_NUMERIC BETWEEN 42.5 AND 42.6)
)
  AND s.SAMPLE_ID IN ('S000200', 'S000199')
ORDER BY s.SAMPLE_ID, pv.VALUE_NUMERIC;


-- QUERY E: Try REQ_ACTIVITY_PARAMETER path
-- ========================================
PROMPT === QUERY E: Are results via REQ_ACTIVITY_PARAMETER? ===
SELECT 
    s.SAMPLE_ID,
    s.NAME as SAMPLE_NAME,
    rt.TASK_ID as TASK_ID_TEXT,
    rt.METHOD_ID as TASK_METHOD,
    ra.NAME as ACTIVITY_NAME,
    ra.METHOD_ID as ACTIVITY_METHOD,
    rap.PARAMETER_ID,
    p.NAME as PARAMETER_NAME,
    pv.VALUE_NUMERIC,
    pv.VALUE_KEY
FROM SAM_SAMPLE s
JOIN REQ_RUNSET_SAMPLE rs ON s.ID = rs.SAMPLE_ID
JOIN REQ_TASK rt ON rs.RUNSET_ID = rt.RUNSET_ID
JOIN REQ_ACTIVITY ra ON rt.ACTIVITY_ID = ra.ID
JOIN REQ_ACT_PARAMETER rap ON ra.ID = rap.ACTIVITY_ID
JOIN COR_PARAMETER p ON rap.PARAMETER_ID = p.ID
LEFT JOIN COR_PARAMETER_VALUE pv ON p.ID = pv.PARENT_IDENTITY
WHERE s.SAMPLE_ID IN ('S000200', 'S000199')
  AND (
    rt.METHOD_ID LIKE '%QAP_PACK_OV%'
    OR ra.METHOD_ID LIKE '%QAP_PACK_OV%'
    OR rt.TASK_NAME LIKE '%OV%'
  )
ORDER BY s.SAMPLE_ID, pv.VALUE_NUMERIC;


-- QUERY F: Just find ANY tasks for these samples with QAP_PACK_OV
-- ========================================
PROMPT === QUERY F: Show me ALL tasks for S000200 and S000199 ===
SELECT 
    s.SAMPLE_ID,
    rs.RUNSET_ID,
    rt.ID as TASK_RAW_ID,
    rt.TASK_ID as TASK_ID_TEXT,
    rt.TASK_NAME,
    rt.METHOD_ID,
    rt.STATUS,
    rt.LIFE_CYCLE_STATE,
    rt.DATE_CREATED,
    ra.NAME as ACTIVITY_NAME,
    ra.METHOD_ID as ACTIVITY_METHOD_ID
FROM SAM_SAMPLE s
JOIN REQ_RUNSET_SAMPLE rs ON s.ID = rs.SAMPLE_ID
JOIN REQ_TASK rt ON rs.RUNSET_ID = rt.RUNSET_ID
LEFT JOIN REQ_ACTIVITY ra ON rt.ACTIVITY_ID = ra.ID
WHERE s.SAMPLE_ID IN ('S000200', 'S000199')
ORDER BY s.SAMPLE_ID, rt.DATE_CREATED DESC;


-- ========================================
-- INTERPRETATION
-- ========================================
/*
If QUERY B returns rows:
  → Results are linked via COR_PARAMETER
  → Use QUERY D for your final report query
  → This is the REQ_TASK path (Version 2 from comprehensive_report_query.sql)

If QUERY B returns no rows:
  → PARENT_IDENTITY is pointing to something else
  → Look at QUERY A output - what are those IDs?
  → We may need to check other tables (REQ_ACTIVITY, REQ_TASK, etc.)

If QUERY D works:
  → SUCCESS! You've found the complete path
  → Samples → Tasks → Parameters → Results
  → Use this as your base query

QUERY F will show you what tasks exist regardless of results
  → This helps verify the tasks are set up correctly
  → Check if METHOD_ID or TASK_NAME contains QAP_PACK_OV
*/