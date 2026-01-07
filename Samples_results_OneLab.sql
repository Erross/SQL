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

SELECT 
    s.SAMPLE_ID,
    s.NAME as SAMPLE_NAME,
    rt.TASK_ID as TASK_ID_TEXT,
    rt.TASK_NAME,
    rt.METHOD_ID,
    rt.LIFE_CYCLE_STATE,
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
  AND rt.TASK_NAME = 'QAP_PACK_OV'
ORDER BY s.SAMPLE_ID, pv.VALUE_NUMERIC;

SELECT 
    s.SAMPLE_ID,
    s.NAME as SAMPLE_NAME,
    rt.ID as TASK_RAW_ID,
    rt.TASK_ID as TASK_ID_TEXT,
    rt.TASK_NAME,
    rt.METHOD_ID,
    p.ID as PARAMETER_RAW_ID,
    p.NAME as PARAMETER_NAME,
    pv.ID as PARAM_VALUE_ID,
    pv.VALUE_NUMERIC,
    pv.VALUE_KEY
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
  AND rt.TASK_NAME = 'QAP_PACK_OV'
ORDER BY s.SAMPLE_ID, pv.VALUE_NUMERIC;

SELECT 
    pv.ID as PARAM_VALUE_ID,
    pv.PARENT_IDENTITY,
    pv.VALUE_NUMERIC,
    pv.VALUE_KEY,
    pv.ITEM_INDEX,
    pv.GROUP_INDEX,
    pv.CONTEXT_ATTRIBUTE,
    pv.VALUE_STRING,
    pv.VALUE_TEXT,
    pv.VALUE_URN,
    pv.VALUE_URN2
FROM COR_PARAMETER_VALUE pv
JOIN COR_PARAMETER p ON pv.PARENT_IDENTITY = p.ID
JOIN REQ_TASK_PARAMETER rtp ON p.ID = rtp.PARAMETER_ID
JOIN REQ_TASK rt ON rtp.TASK_ID = rt.ID
WHERE rt.TASK_NAME = 'QAP_PACK_OV'
  AND p.NAME = 'Percent'
  AND (
    (pv.VALUE_NUMERIC BETWEEN 41.0 AND 41.1)
    OR (pv.VALUE_NUMERIC BETWEEN 42.5 AND 42.6)
  )
ORDER BY pv.VALUE_NUMERIC;

SELECT 
    -- Extract the sample ID from the comma-separated SAMPLE_LIST based on ITEM_INDEX
    CASE 
        WHEN pv.ITEM_INDEX = 0 THEN REGEXP_SUBSTR(rt.SAMPLE_LIST, '[^,]+', 1, 1)
        WHEN pv.ITEM_INDEX = 1 THEN REGEXP_SUBSTR(rt.SAMPLE_LIST, '[^,]+', 1, 2)
        WHEN pv.ITEM_INDEX = 2 THEN REGEXP_SUBSTR(rt.SAMPLE_LIST, '[^,]+', 1, 3)
        WHEN pv.ITEM_INDEX = 3 THEN REGEXP_SUBSTR(rt.SAMPLE_LIST, '[^,]+', 1, 4)
    END as MATCHED_SAMPLE_ID,
    rt.TASK_ID as TASK_ID_TEXT,
    rt.TASK_NAME,
    rt.METHOD_ID,
    rt.SAMPLE_LIST,
    pv.ITEM_INDEX,
    pv.VALUE_NUMERIC,
    pv.VALUE_TEXT as FORMATTED_RESULT,
    p.NAME as PARAMETER_NAME
FROM COR_PARAMETER_VALUE pv
JOIN COR_PARAMETER p ON pv.PARENT_IDENTITY = p.ID
JOIN REQ_TASK_PARAMETER rtp ON p.ID = rtp.PARAMETER_ID
JOIN REQ_TASK rt ON rtp.TASK_ID = rt.ID
WHERE rt.TASK_NAME = 'QAP_PACK_OV'
  AND p.NAME = 'Percent'
  AND (
    (pv.VALUE_NUMERIC BETWEEN 41.0 AND 41.1)
    OR (pv.VALUE_NUMERIC BETWEEN 42.5 AND 42.6)
  )
ORDER BY pv.ITEM_INDEX;

SELECT 
    -- Extract the Nth sample from SAMPLE_LIST where N = ITEM_INDEX + 1 (since ITEM_INDEX is 0-based)
    REGEXP_SUBSTR(rt.SAMPLE_LIST, '[^,]+', 1, pv.ITEM_INDEX + 1) as MATCHED_SAMPLE_ID,
    rt.TASK_ID as TASK_ID_TEXT,
    rt.TASK_NAME,
    rt.METHOD_ID,
    rt.SAMPLE_LIST,
    pv.ITEM_INDEX,
    pv.VALUE_NUMERIC,
    pv.VALUE_TEXT as FORMATTED_RESULT,
    p.NAME as PARAMETER_NAME
FROM COR_PARAMETER_VALUE pv
JOIN COR_PARAMETER p ON pv.PARENT_IDENTITY = p.ID
JOIN REQ_TASK_PARAMETER rtp ON p.ID = rtp.PARAMETER_ID
JOIN REQ_TASK rt ON rtp.TASK_ID = rt.ID
WHERE rt.TASK_NAME = 'QAP_PACK_OV'
  AND p.NAME = 'Percent'
  AND (
    (pv.VALUE_NUMERIC BETWEEN 41.0 AND 41.1)
    OR (pv.VALUE_NUMERIC BETWEEN 42.5 AND 42.6)
  )
ORDER BY pv.ITEM_INDEX;

SELECT 
    -- Sample Information
    REGEXP_SUBSTR(rt.SAMPLE_LIST, '[^,]+', 1, pv.ITEM_INDEX + 1) as "Sample ID",
    s.NAME as "Sample Name",
    s.SAMPLE_ID as "Sample ID Verified",
    ms.SAMPLE_ID as "Master Sample ID",
    
    -- Sampling Point Information
    sl.NAME as "Sampling Point",
    sl.DESCRIPTION as "Sampling Point Description",
    
    -- Location
    loc.NAME as "Location",
    
    -- Owner Information (Line-1 Owner)
    u.FULL_NAME as "Line-1 Owner",
    
    -- Project Information
    proj.NAME as "Project",
    proj.PROJECT_CODE as "Project Code",
    
    -- Task Plan Information
    runset.NAME as "Task Plan",
    runset.RUNSET_ID as "Task Plan ID",
    
    -- Task Information
    rt.TASK_ID as "Task ID",
    rt.TASK_NAME as "Task Name",
    rt.METHOD_ID as "Method ID",
    rt.LIFE_CYCLE_STATE as "Task Status",
    rt.COMPLETION_DATE as "Task Completion Date",
    
    -- Activity Information
    ra.NAME as "Activity Name",
    
    -- Specification Method
    sm.NAME as "Spec Method Name",
    sm.METHOD_ID as "Spec Method ID",
    
    -- Characteristic/Spec Group Information
    smc.COMPONENT as "Spec Group",
    p.NAME as "Characteristic",
    p.DESCRIPTION as "Characteristic Description",
    
    -- Result Information
    pv.VALUE_KEY as "Result Key",
    pv.VALUE_NUMERIC as "Result",
    pv.VALUE_TEXT as "Formatted Result",
    pv.INTERPRETATION as "Compose Details",
    
    -- Additional Context
    pv.ITEM_INDEX as "Item Index",
    rt.SAMPLE_LIST as "Sample List"
    
FROM COR_PARAMETER_VALUE pv
JOIN COR_PARAMETER p ON pv.PARENT_IDENTITY = p.ID
JOIN REQ_TASK_PARAMETER rtp ON p.ID = rtp.PARAMETER_ID
JOIN REQ_TASK rt ON rtp.TASK_ID = rt.ID

-- Runset/Task Plan
LEFT JOIN REQ_RUNSET runset ON rt.RUNSET_ID = runset.ID

-- Activity
LEFT JOIN REQ_ACTIVITY ra ON rt.ACTIVITY_ID = ra.ID

-- Specification Method and Characteristics
LEFT JOIN SAM_SPEC_METHOD sm ON rt.SPECIFICATION_METHOD_ID = sm.ID
LEFT JOIN SAM_SPEC_MTHD_CHAR smc ON sm.ID = smc.SPECIFICATION_METHOD_ID AND smc.PARAMETER_ID = p.ID

-- Now join to actual SAM_SAMPLE using the parsed sample ID
LEFT JOIN SAM_SAMPLE s ON s.SAMPLE_ID = REGEXP_SUBSTR(rt.SAMPLE_LIST, '[^,]+', 1, pv.ITEM_INDEX + 1)

-- Master Sample
LEFT JOIN SAM_SAMPLE ms ON s.MASTER_SAMPLE_ID = ms.ID

-- Locations
LEFT JOIN RES_LOCATION sl ON s.SAMPLING_LOCATION_ID = sl.ID
LEFT JOIN RES_LOCATION loc ON s.LOCATION_ID = loc.ID

-- Owner
LEFT JOIN SEC_USER u ON s.OWNER_ID = u.ID

-- Project
LEFT JOIN RES_PROJECT proj ON s.PROJECT_ID = proj.ID

WHERE rt.TASK_NAME = 'QAP_PACK_OV'
  AND p.NAME = 'Percent'
  AND REGEXP_SUBSTR(rt.SAMPLE_LIST, '[^,]+', 1, pv.ITEM_INDEX + 1) IN ('S000200', 'S000199')
  
ORDER BY s.SAMPLE_ID, pv.ITEM_INDEX;