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

-- ========================================
-- FINAL COMPREHENSIVE REPORT QUERY
-- Using ONLY verified columns from DDL
-- ========================================

SELECT 
    -- Sample Information
    REGEXP_SUBSTR(rt.SAMPLE_LIST, '[^,]+', 1, pv.ITEM_INDEX + 1) as "Sample ID",
    s.NAME as "Sample Name",
    ms.SAMPLE_ID as "Master Sample ID",
    
    -- Sampling Point Information
    sl.NAME as "Sampling Point",
    sl.DESCRIPTION as "Sampling Point Description",
    
    -- Location
    loc.NAME as "Location",
    loc.DESCRIPTION as "Location Description",
    
    -- Owner Information (Line-1 Owner)
    u.NAME as "Line-1 Owner",
    u.USERNAME as "Owner Username",
    u.FIRST_NAME as "Owner First Name",
    u.LAST_NAME as "Owner Last Name",
    
    -- Project Information
    proj.NAME as "Project",
    proj.DESCRIPTION as "Project Description",
    
    -- Task Plan Information
    runset.NAME as "Task Plan",
    runset.RUNSET_ID as "Task Plan ID",
    runset.LIFE_CYCLE_STATE as "Task Plan State",
    
    -- Task Information
    rt.TASK_ID as "Task ID",
    rt.TASK_NAME as "Task Name",
    rt.METHOD_ID as "Method ID",
    rt.LIFE_CYCLE_STATE as "Task Status",
    rt.DATE_CREATED as "Task Created",
    rt.COMPLETION_DATE as "Task Completion Date",
    
    -- Activity Information
    ra.NAME as "Activity Name",
    ra.METHOD_ID as "Activity Method ID",
    ra.DESCRIPTION as "Activity Description",
    
    -- Characteristic Information
    p.NAME as "Characteristic",
    p.DESCRIPTION as "Characteristic Description",
    smc.COMPONENT as "Spec Group",
    smc.TARGET as "Target",
    smc.LOWER_LIMIT as "Lower Limit",
    smc.UPPER_LIMIT as "Upper Limit",
    
    -- Result Information
    pv.VALUE_KEY as "Result Key",
    pv.VALUE_NUMERIC as "Result",
    pv.VALUE_TEXT as "Formatted Result",
    pv.VALUE_STRING as "Result String",
    pv.INTERPRETATION as "Compose Details",
    
    -- Additional Context
    pv.ITEM_INDEX as "Item Index",
    pv.GROUP_INDEX as "Group Index",
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

-- Join to actual SAM_SAMPLE using the parsed sample ID
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
  
ORDER BY "Sample ID", pv.ITEM_INDEX;

//prior gives multi row answer with some bullshit

SELECT 
    -- Sample Information
    REGEXP_SUBSTR(rt.SAMPLE_LIST, '[^,]+', 1, pv.ITEM_INDEX + 1) as "Sample ID",
    s.NAME as "Sample Name",
    ms.SAMPLE_ID as "Master Sample ID",
    
    -- Sampling Point Information
    sl.NAME as "Sampling Point",
    sl.DESCRIPTION as "Sampling Point Description",
    
    -- Location
    loc.NAME as "Location",
    loc.DESCRIPTION as "Location Description",
    
    -- Owner Information (Line-1 Owner)
    u.NAME as "Line-1 Owner",
    u.USERNAME as "Owner Username",
    u.FIRST_NAME as "Owner First Name",
    u.LAST_NAME as "Owner Last Name",
    
    -- Project Information
    proj.NAME as "Project",
    proj.DESCRIPTION as "Project Description",
    
    -- Task Plan Information
    runset.NAME as "Task Plan",
    runset.RUNSET_ID as "Task Plan ID",
    runset.LIFE_CYCLE_STATE as "Task Plan State",
    
    -- Task Information
    rt.TASK_ID as "Task ID",
    rt.TASK_NAME as "Task Name",
    rt.METHOD_ID as "Method ID",
    rt.LIFE_CYCLE_STATE as "Task Status",
    rt.DATE_CREATED as "Task Created",
    rt.COMPLETION_DATE as "Task Completion Date",
    
    -- Activity Information
    ra.NAME as "Activity Name",
    ra.METHOD_ID as "Activity Method ID",
    ra.DESCRIPTION as "Activity Description",
    
    -- Characteristic Information
    p.NAME as "Characteristic",
    p.DESCRIPTION as "Characteristic Description",
    smc.COMPONENT as "Spec Group",
    smc.TARGET as "Target",
    smc.LOWER_LIMIT as "Lower Limit",
    smc.UPPER_LIMIT as "Upper Limit",
    
    -- Result Information
    pv.VALUE_KEY as "Result Key",
    pv.VALUE_NUMERIC as "Result",
    pv.VALUE_TEXT as "Formatted Result",
    pv.VALUE_STRING as "Result String",
    pv.INTERPRETATION as "Compose Details",
    
    -- Additional Context
    pv.ITEM_INDEX as "Item Index",
    pv.GROUP_INDEX as "Group Index",
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

-- Join to actual SAM_SAMPLE using the parsed sample ID
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
  AND pv.VALUE_KEY = 'A'
  AND REGEXP_SUBSTR(rt.SAMPLE_LIST, '[^,]+', 1, pv.ITEM_INDEX + 1) IN ('S000200', 'S000199')
  
ORDER BY "Sample ID", pv.ITEM_INDEX;

--NEW ATTEMPT

-- ========================================
-- FINAL COMPREHENSIVE REPORT QUERY
-- Based on proven working query patterns
-- One row per sample per test result
-- ========================================

WITH sample_properties AS (
  -- Get all custom properties for samples
  SELECT
    oi.object_id AS sample_raw_id,
    
    MAX(CASE WHEN p.display_label = 'Sampling Point'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1),
                           TO_CHAR(pv.number_value))
        END) AS sampling_point,
    
    MAX(CASE WHEN p.display_label = 'Sampling Point Description'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS sampling_point_description,
    
    MAX(CASE WHEN p.display_label = 'Line'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1),
                           TO_CHAR(pv.number_value))
        END) AS line,
    
    MAX(CASE WHEN p.display_label = 'Product Code'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1),
                           TO_CHAR(pv.number_value))
        END) AS product_code,
    
    MAX(CASE WHEN p.display_label = 'Product Description'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS product_description,
    
    MAX(CASE WHEN p.display_label = 'Cig Product Code'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1),
                           TO_CHAR(pv.number_value))
        END) AS cig_product_code,
    
    MAX(CASE WHEN p.display_label = 'Cig Product Description'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS cig_product_description,
    
    MAX(CASE WHEN p.display_label = 'Spec group'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS spec_group
        
  FROM cor_class_identity ci
  JOIN cor_object_identity oi ON oi.class_identity_id = ci.id
  JOIN cor_property_value pv ON pv.object_identity_id = oi.id
  JOIN cor_property p ON p.name = pv.property_id
  WHERE ci.table_name = 'sam_sample'
    AND p.display_label IN (
      'Sampling Point',
      'Sampling Point Description', 
      'Line',
      'Product Code',
      'Product Description',
      'Cig Product Code',
      'Cig Product Description',
      'Spec group'
    )
  GROUP BY oi.object_id
)

SELECT 
    -- Sample Information
    s.SAMPLE_ID as "Sample ID",
    s.NAME as "Sample Name",
    ms.SAMPLE_ID as "Master Sample ID",
    
    -- Sampling Point Information (from properties)
    sp.sampling_point as "Sampling Point",
    sp.sampling_point_description as "Sampling Point Description",
    
    -- Line
    sp.line as "Line",
    
    -- Location Information (from location table)
    loc.NAME as "Location",
    loc.DESCRIPTION as "Location Description",
    
    -- Owner Information
    u.NAME as "Line-1 Owner",
    u.USERNAME as "Owner Username",
    
    -- Product Information (from properties)
    sp.product_code as "Product Code",
    sp.product_description as "Product Description",
    sp.cig_product_code as "CIG Product Code",
    sp.cig_product_description as "CIG Product Description",
    
    -- Spec Group (from properties)
    sp.spec_group as "Spec Group",
    
    -- Project Information
    proj.NAME as "Project",
    proj.DESCRIPTION as "Project Description",
    
    -- Task Plan Information
    runset.NAME as "Task Plan",
    runset.RUNSET_ID as "Task Plan ID",
    
    -- Task Information
    rt.TASK_ID as "Task ID",
    rt.TASK_NAME as "Task Name",
    rt.METHOD_ID as "Method ID",
    rt.LIFE_CYCLE_STATE as "Task Status",
    rt.DATE_CREATED as "Task Created",
    rt.COMPLETION_DATE as "Task Completed",
    
    -- Activity Information
    ra.NAME as "Activity Name",
    ra.METHOD_ID as "Activity Method ID",
    
    -- Characteristic Information
    p.NAME as "Characteristic",
    p.DESCRIPTION as "Characteristic Description",
    smc.COMPONENT as "Spec Group From Method",
    smc.TARGET as "Target",
    smc.LOWER_LIMIT as "Lower Limit",
    smc.UPPER_LIMIT as "Upper Limit",
    
    -- Result Information
    pv.VALUE_KEY as "Result Key",
    pv.VALUE_NUMERIC as "Result",
    pv.VALUE_TEXT as "Formatted Result",
    pv.INTERPRETATION as "Compose Details",
    
    -- Additional Context
    pv.ITEM_INDEX as "Item Index"
    
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

-- Join to SAM_SAMPLE using the ITEM_INDEX match to SAMPLE_LIST
-- This is the key join we discovered!
LEFT JOIN SAM_SAMPLE s ON s.SAMPLE_ID = REGEXP_SUBSTR(rt.SAMPLE_LIST, '[^,]+', 1, pv.ITEM_INDEX + 1)

-- Master Sample
LEFT JOIN SAM_SAMPLE ms ON s.MASTER_SAMPLE_ID = ms.ID

-- Location
LEFT JOIN RES_LOCATION loc ON s.LOCATION_ID = loc.ID

-- Owner
LEFT JOIN SEC_USER u ON s.OWNER_ID = u.ID

-- Project
LEFT JOIN RES_PROJECT proj ON s.PROJECT_ID = proj.ID

-- Sample Properties
LEFT JOIN sample_properties sp ON sp.sample_raw_id = s.ID

WHERE rt.TASK_NAME = 'QAP_PACK_OV'
  AND p.NAME = 'Percent'
  AND pv.VALUE_KEY = 'A'
  AND REGEXP_SUBSTR(rt.SAMPLE_LIST, '[^,]+', 1, pv.ITEM_INDEX + 1) IN ('S000200', 'S000199')
  
ORDER BY s.SAMPLE_ID, pv.ITEM_INDEX;

--FINAL FOR 2 SAMPLES

-- ========================================
-- FINAL REPORT QUERY
-- Exact column names as specified
-- ========================================

WITH sample_properties AS (
  SELECT
    oi.object_id AS sample_raw_id,
    
    MAX(CASE WHEN p.display_label = 'Sampling Point'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1),
                           TO_CHAR(pv.number_value))
        END) AS sampling_point,
    
    MAX(CASE WHEN p.display_label = 'Sampling Point Description'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS sampling_point_description,
    
    MAX(CASE WHEN p.display_label = 'Line'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1),
                           TO_CHAR(pv.number_value))
        END) AS line,
    
    MAX(CASE WHEN p.display_label = 'Product Code'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1),
                           TO_CHAR(pv.number_value))
        END) AS product_code,
    
    MAX(CASE WHEN p.display_label = 'Product Description'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS product_description,
    
    MAX(CASE WHEN p.display_label = 'Cig Product Code'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1),
                           TO_CHAR(pv.number_value))
        END) AS cig_product_code,
    
    MAX(CASE WHEN p.display_label = 'Cig Product Description'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS cig_product_description,
    
    MAX(CASE WHEN p.display_label = 'Spec group'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS spec_group
        
  FROM cor_class_identity ci
  JOIN cor_object_identity oi ON oi.class_identity_id = ci.id
  JOIN cor_property_value pv ON pv.object_identity_id = oi.id
  JOIN cor_property p ON p.name = pv.property_id
  WHERE ci.table_name = 'sam_sample'
    AND p.display_label IN (
      'Sampling Point',
      'Sampling Point Description', 
      'Line',
      'Product Code',
      'Product Description',
      'Cig Product Code',
      'Cig Product Description',
      'Spec group'
    )
  GROUP BY oi.object_id
)

SELECT 
    s.NAME as "Sample Name",
    s.SAMPLE_ID as "Sample ID",
    ms.SAMPLE_ID as "Master Sample ID",
    sp.sampling_point as "Sampling point",
    sp.sampling_point_description as "Sampling point description",
    sp.line as "LINE-1",
    u.NAME as "Owner",
    sp.product_code as "Product Code",
    sp.product_description as "Product Description",
    sp.cig_product_code as "CIG_PRODUCT_CODE",
    sp.cig_product_description as "CIG_PRODUCT_DESCRIPTION",
    sp.spec_group as "Spec_Group",
    proj.NAME as "Task Plan Project",
    rt.LIFE_CYCLE_STATE as "Task Status",
    p.NAME as "Characteristic",
    pv.INTERPRETATION as "Compose Details",
    pv.VALUE_STRING as "Result",
    pv.VALUE_TEXT as "Formatted result"
    
FROM COR_PARAMETER_VALUE pv
JOIN COR_PARAMETER p ON pv.PARENT_IDENTITY = p.ID
JOIN REQ_TASK_PARAMETER rtp ON p.ID = rtp.PARAMETER_ID
JOIN REQ_TASK rt ON rtp.TASK_ID = rt.ID
LEFT JOIN REQ_RUNSET runset ON rt.RUNSET_ID = runset.ID
LEFT JOIN REQ_ACTIVITY ra ON rt.ACTIVITY_ID = ra.ID
LEFT JOIN SAM_SPEC_METHOD sm ON rt.SPECIFICATION_METHOD_ID = sm.ID
LEFT JOIN SAM_SPEC_MTHD_CHAR smc ON sm.ID = smc.SPECIFICATION_METHOD_ID AND smc.PARAMETER_ID = p.ID
LEFT JOIN SAM_SAMPLE s ON s.SAMPLE_ID = REGEXP_SUBSTR(rt.SAMPLE_LIST, '[^,]+', 1, pv.ITEM_INDEX + 1)
LEFT JOIN SAM_SAMPLE ms ON s.MASTER_SAMPLE_ID = ms.ID
LEFT JOIN SEC_USER u ON s.OWNER_ID = u.ID
LEFT JOIN RES_PROJECT proj ON s.PROJECT_ID = proj.ID
LEFT JOIN sample_properties sp ON sp.sample_raw_id = s.ID

WHERE rt.TASK_NAME = 'QAP_PACK_OV'
  AND p.NAME = 'Percent'
  AND pv.VALUE_KEY = 'A'
  AND REGEXP_SUBSTR(rt.SAMPLE_LIST, '[^,]+', 1, pv.ITEM_INDEX + 1) IN ('S000200', 'S000199')
  
ORDER BY s.SAMPLE_ID;


-- ========================================
-- COLUMN MAPPINGS
-- ========================================
/*
Sample Name              -> SAM_SAMPLE.NAME
Sample ID                -> SAM_SAMPLE.SAMPLE_ID
Master Sample ID         -> Master SAM_SAMPLE.SAMPLE_ID (via MASTER_SAMPLE_ID FK)
Sampling point           -> Property: 'Sampling Point'
Sampling point description -> Property: 'Sampling Point Description'
LINE-1                   -> Property: 'Line'
Owner                    -> SEC_USER.NAME
Product Code             -> Property: 'Product Code'
Product Description      -> Property: 'Product Description'
CIG_PRODUCT_CODE         -> Property: 'Cig Product Code'
CIG_PRODUCT_DESCRIPTION  -> Property: 'Cig Product Description'
Spec_Group               -> Property: 'Spec group'
Task Plan Project        -> RES_PROJECT.NAME
Task Status              -> REQ_TASK.LIFE_CYCLE_STATE
Characteristic           -> COR_PARAMETER.NAME
Compose Details          -> COR_PARAMETER_VALUE.INTERPRETATION
Result                   -> COR_PARAMETER_VALUE.VALUE_STRING (full precision: 41.0229645...)
Formatted result         -> COR_PARAMETER_VALUE.VALUE_TEXT (rounded: 41.02)
*/

-- ========================================
-- GENERALIZED REPORT QUERY
-- With exact column names
-- Remove sample filter to get all samples
-- Change TASK_NAME to query different tests
-- ========================================

WITH sample_properties AS (
  SELECT
    oi.object_id AS sample_raw_id,
    
    MAX(CASE WHEN p.display_label = 'Sampling Point'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1),
                           TO_CHAR(pv.number_value))
        END) AS sampling_point,
    
    MAX(CASE WHEN p.display_label = 'Sampling Point Description'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS sampling_point_description,
    
    MAX(CASE WHEN p.display_label = 'Line'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1),
                           TO_CHAR(pv.number_value))
        END) AS line,
    
    MAX(CASE WHEN p.display_label = 'Product Code'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1),
                           TO_CHAR(pv.number_value))
        END) AS product_code,
    
    MAX(CASE WHEN p.display_label = 'Product Description'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS product_description,
    
    MAX(CASE WHEN p.display_label = 'Cig Product Code'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1),
                           TO_CHAR(pv.number_value))
        END) AS cig_product_code,
    
    MAX(CASE WHEN p.display_label = 'Cig Product Description'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS cig_product_description,
    
    MAX(CASE WHEN p.display_label = 'Spec group'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS spec_group
        
  FROM cor_class_identity ci
  JOIN cor_object_identity oi ON oi.class_identity_id = ci.id
  JOIN cor_property_value pv ON pv.object_identity_id = oi.id
  JOIN cor_property p ON p.name = pv.property_id
  WHERE ci.table_name = 'sam_sample'
    AND p.display_label IN (
      'Sampling Point',
      'Sampling Point Description', 
      'Line',
      'Product Code',
      'Product Description',
      'Cig Product Code',
      'Cig Product Description',
      'Spec group'
    )
  GROUP BY oi.object_id
)

SELECT 
    s.NAME as "Sample Name",
    s.SAMPLE_ID as "Sample ID",
    ms.SAMPLE_ID as "Master Sample ID",
    sp.sampling_point as "Sampling point",
    sp.sampling_point_description as "Sampling point description",
    sp.line as "LINE-1",
    u.NAME as "Owner",
    sp.product_code as "Product Code",
    sp.product_description as "Product Description",
    sp.cig_product_code as "CIG_PRODUCT_CODE",
    sp.cig_product_description as "CIG_PRODUCT_DESCRIPTION",
    sp.spec_group as "Spec_Group",
    proj.NAME as "Task Plan Project",
    rt.LIFE_CYCLE_STATE as "Task Status",
    p.NAME as "Characteristic",
    pv.INTERPRETATION as "Compose Details",
    pv.VALUE_STRING as "Result",
    pv.VALUE_TEXT as "Formatted result"
    
FROM COR_PARAMETER_VALUE pv
JOIN COR_PARAMETER p ON pv.PARENT_IDENTITY = p.ID
JOIN REQ_TASK_PARAMETER rtp ON p.ID = rtp.PARAMETER_ID
JOIN REQ_TASK rt ON rtp.TASK_ID = rt.ID
LEFT JOIN REQ_RUNSET runset ON rt.RUNSET_ID = runset.ID
LEFT JOIN REQ_ACTIVITY ra ON rt.ACTIVITY_ID = ra.ID
LEFT JOIN SAM_SPEC_METHOD sm ON rt.SPECIFICATION_METHOD_ID = sm.ID
LEFT JOIN SAM_SPEC_MTHD_CHAR smc ON sm.ID = smc.SPECIFICATION_METHOD_ID AND smc.PARAMETER_ID = p.ID
LEFT JOIN SAM_SAMPLE s ON s.SAMPLE_ID = REGEXP_SUBSTR(rt.SAMPLE_LIST, '[^,]+', 1, pv.ITEM_INDEX + 1)
LEFT JOIN SAM_SAMPLE ms ON s.MASTER_SAMPLE_ID = ms.ID
LEFT JOIN SEC_USER u ON s.OWNER_ID = u.ID
LEFT JOIN RES_PROJECT proj ON s.PROJECT_ID = proj.ID
LEFT JOIN sample_properties sp ON sp.sample_raw_id = s.ID

WHERE rt.TASK_NAME = 'QAP_PACK_OV'      -- Change this for different tests
  --AND p.NAME = 'Percent'  AND               -- Change this for different parameters
  pv.VALUE_KEY = 'A'                 -- Change/remove if needed for different tests
  -- AND s.SAMPLE_ID IN ('S000200', 'S000199')  -- Uncomment to filter specific samples
  
ORDER BY s.SAMPLE_ID;


-- ========================================
-- CUSTOMIZATION GUIDE
-- ========================================
/*
TO QUERY DIFFERENT TESTS:
1. Change: WHERE rt.TASK_NAME = 'YOUR_TEST_NAME'
2. May need to adjust: p.NAME = 'YourParameterName'
3. Check if VALUE_KEY needs to change

TO GET ALL SAMPLES:
- Remove or comment out the sample filter line

TO ADD MORE PROPERTIES:
- Add CASE statement to sample_properties CTE
- Use exact display_label from COR_PROPERTY table

TO FILTER BY DATE RANGE:
- Add: AND rt.DATE_CREATED BETWEEN date1 AND date2
- Or: AND rt.COMPLETION_DATE >= date1

TROUBLESHOOTING:
- If duplicate rows: Check VALUE_KEY filter
- If missing results: Remove VALUE_KEY filter temporarily
- If wrong data: Verify TASK_NAME and parameter NAME
*/

WITH sample_properties AS (
  SELECT
    oi.object_id AS sample_raw_id,
    
    MAX(CASE WHEN p.display_label = 'Sampling Point'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1),
                           TO_CHAR(pv.number_value))
        END) AS sampling_point,
    
    MAX(CASE WHEN p.display_label = 'Sampling Point Description'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS sampling_point_description,
    
    MAX(CASE WHEN p.display_label = 'Line'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1),
                           TO_CHAR(pv.number_value))
        END) AS line,
    
    MAX(CASE WHEN p.display_label = 'Product Code'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1),
                           TO_CHAR(pv.number_value))
        END) AS product_code,
    
    MAX(CASE WHEN p.display_label = 'Product Description'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS product_description,
    
    MAX(CASE WHEN p.display_label = 'Cig Product Code'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1),
                           TO_CHAR(pv.number_value))
        END) AS cig_product_code,
    
    MAX(CASE WHEN p.display_label = 'Cig Product Description'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS cig_product_description,
    
    MAX(CASE WHEN p.display_label = 'Spec group'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS spec_group
        
  FROM cor_class_identity ci
  JOIN cor_object_identity oi ON oi.class_identity_id = ci.id
  JOIN cor_property_value pv ON pv.object_identity_id = oi.id
  JOIN cor_property p ON p.name = pv.property_id
  WHERE ci.table_name = 'sam_sample'
    AND p.display_label IN (
      'Sampling Point',
      'Sampling Point Description', 
      'Line',
      'Product Code',
      'Product Description',
      'Cig Product Code',
      'Cig Product Description',
      'Spec group'
    )
  GROUP BY oi.object_id
)

SELECT 
    s.NAME as "Sample Name",
    s.SAMPLE_ID as "Sample ID",
    ms.SAMPLE_ID as "Master Sample ID",
    sp.sampling_point as "Sampling point",
    sp.sampling_point_description as "Sampling point description",
    sp.line as "LINE-1",
    u.NAME as "Owner",
    sp.product_code as "Product Code",
    sp.product_description as "Product Description",
    sp.cig_product_code as "CIG_PRODUCT_CODE",
    sp.cig_product_description as "CIG_PRODUCT_DESCRIPTION",
    sp.spec_group as "Spec_Group",
    proj.NAME as "Task Plan Project",
    runset.RUNSET_ID as "Task Plan ID",
    rt.LIFE_CYCLE_STATE as "Task Status",
    p.DISPLAY_NAME as "Characteristic",
    pv.INTERPRETATION as "Compose Details",
    pv.VALUE_STRING as "Result",
    pv.VALUE_TEXT as "Formatted result",
    cs.NAME as "Collaboration Space"
    
FROM COR_PARAMETER_VALUE pv
JOIN COR_PARAMETER p ON pv.PARENT_IDENTITY = p.ID
JOIN REQ_TASK_PARAMETER rtp ON p.ID = rtp.PARAMETER_ID
JOIN REQ_TASK rt ON rtp.TASK_ID = rt.ID
LEFT JOIN REQ_RUNSET runset ON rt.RUNSET_ID = runset.ID
LEFT JOIN REQ_ACTIVITY ra ON rt.ACTIVITY_ID = ra.ID
LEFT JOIN SAM_SPEC_METHOD sm ON rt.SPECIFICATION_METHOD_ID = sm.ID
LEFT JOIN SAM_SPEC_MTHD_CHAR smc ON sm.ID = smc.SPECIFICATION_METHOD_ID AND smc.PARAMETER_ID = p.ID
LEFT JOIN SAM_SAMPLE s ON s.SAMPLE_ID = REGEXP_SUBSTR(rt.SAMPLE_LIST, '[^,]+', 1, pv.ITEM_INDEX + 1)
LEFT JOIN SAM_SAMPLE ms ON s.MASTER_SAMPLE_ID = ms.ID
LEFT JOIN SEC_USER u ON s.OWNER_ID = u.ID
LEFT JOIN RES_PROJECT proj ON s.PROJECT_ID = proj.ID
LEFT JOIN sample_properties sp ON sp.sample_raw_id = s.ID

-- Link to Collaboration Space (can link via SAMPLE or RUNSET)
-- Option 1: Link via Sample
LEFT JOIN COSPC_OBJECT_IDENTITY coi_sample ON coi_sample.OBJECT_ID = s.ID
-- Option 2: Link via Runset (uncomment if samples don't have collab space links)
-- LEFT JOIN COSPC_OBJECT_IDENTITY coi_runset ON coi_runset.OBJECT_ID = runset.ID
LEFT JOIN SEC_COLLAB_SPACE cs ON cs.ID = coi_sample.COLLABORATIVE_SPACE_ID
-- If using runset instead: cs.ID = coi_runset.COLLABORATIVE_SPACE_ID

WHERE pv.VALUE_KEY = 'A'
  AND s.SAMPLE_ID IS NOT NULL
  AND rt.LIFE_CYCLE_STATE IN ('Released', 'Completed')
  AND p.VALUE_TYPE NOT IN ('Vocabulary')
  AND pv.VALUE_STRING IS NOT NULL
  AND cs.NAME = 'YOUR_COLLAB_SPACE_NAME_HERE'  -- ← CHANGE THIS!
  
ORDER BY s.SAMPLE_ID;