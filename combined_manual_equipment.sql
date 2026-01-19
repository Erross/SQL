-- ========================================
-- COMBINED MANUAL + EQUIPMENT TEST RESULTS
-- ========================================

WITH sample_properties AS (
  SELECT
    oi.object_id AS sample_raw_id,
    MAX(CASE WHEN p.display_label = 'Sampling Point'
             THEN COALESCE(pv.string_value, DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1), TO_CHAR(pv.number_value))
        END) AS sampling_point,
    MAX(CASE WHEN p.display_label = 'Sampling Point Description'
             THEN COALESCE(pv.string_value, DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS sampling_point_description,
    MAX(CASE WHEN p.display_label = 'Line'
             THEN COALESCE(pv.string_value, DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1), TO_CHAR(pv.number_value))
        END) AS line,
    MAX(CASE WHEN p.display_label = 'Product Code'
             THEN COALESCE(pv.string_value, DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1), TO_CHAR(pv.number_value))
        END) AS product_code,
    MAX(CASE WHEN p.display_label = 'Product Description'
             THEN COALESCE(pv.string_value, DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS product_description,
    MAX(CASE WHEN p.display_label = 'Cig Product Code'
             THEN COALESCE(pv.string_value, DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS cig_product_code,
    MAX(CASE WHEN p.display_label = 'Cig Product Description'
             THEN COALESCE(pv.string_value, DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS cig_product_description,
    MAX(CASE WHEN p.display_label = 'Spec group'
             THEN COALESCE(pv.string_value, DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS spec_group
  FROM cor_class_identity ci
  JOIN cor_object_identity oi ON oi.class_identity_id = ci.id
  JOIN cor_property_value pv ON pv.object_identity_id = oi.id
  JOIN cor_property p ON p.name = pv.property_id
  WHERE ci.table_name = 'sam_sample'
    AND p.display_label IN ('Sampling Point', 'Sampling Point Description', 'Line', 'Product Code', 'Product Description', 'Cig Product Code', 'Cig Product Description', 'Spec group')
  GROUP BY oi.object_id
)

-- MANUAL TEST RESULTS
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
    pv.VALUE_TEXT as "Formatted result",
    'MANUAL' as "Result Source"
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
WHERE pv.VALUE_KEY = 'A'
  AND runset.RUNSET_ID = 'TP002'

UNION ALL

-- EQUIPMENT MEASUREMENT RESULTS
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
    MAX(CASE WHEN pv.VALUE_STRING IS NOT NULL THEN pv.VALUE_STRING END) as "Characteristic",
    NULL as "Compose Details",
    TO_CHAR(MAX(CASE WHEN pv.VALUE_NUMERIC IS NOT NULL THEN pv.VALUE_NUMERIC END)) as "Result",
    TO_CHAR(MAX(CASE WHEN pv.VALUE_NUMERIC IS NOT NULL THEN pv.VALUE_NUMERIC END)) as "Formatted result",
    'EQUIPMENT' as "Result Source"
FROM COR_PARAMETER_VALUE pv
JOIN PEX_PROC_ELEM_EXEC_PARAM peep ON peep.ID = pv.PARENT_IDENTITY
JOIN PEX_PROC_ELEM_EXEC pee ON pee.ID = peep.PARENT_ID
JOIN PEX_PROC_EXEC pe ON pe.ID = pee.PARENT_ID
JOIN RES_RETRIEVAL_CONTEXT ctx ON ctx.CONTEXT = 
    'urn:pexelement:' || LOWER(SUBSTR(RAWTOHEX(pee.ID), 1, 8) || '-' || SUBSTR(RAWTOHEX(pee.ID), 9, 4) || '-' || SUBSTR(RAWTOHEX(pee.ID), 13, 4) || '-' || SUBSTR(RAWTOHEX(pee.ID), 17, 4) || '-' || SUBSTR(RAWTOHEX(pee.ID), 21, 12))
JOIN RES_MEASUREMENTSAMPLE meas_s ON meas_s.CONTEXT_ID = ctx.ID
JOIN RES_MEASUREMENT m ON m.ID = meas_s.MEASUREMENT_ID
JOIN SAM_SAMPLE s ON s.ID = meas_s.MAPPED_SAMPLE_ID
JOIN REQ_TASK rt ON INSTR(',' || rt.SAMPLE_LIST || ',', ',' || s.SAMPLE_ID || ',') > 0
JOIN REQ_RUNSET runset ON runset.ID = rt.RUNSET_ID
LEFT JOIN SAM_SAMPLE ms ON s.MASTER_SAMPLE_ID = ms.ID
LEFT JOIN SEC_USER u ON s.OWNER_ID = u.ID
LEFT JOIN RES_PROJECT proj ON s.PROJECT_ID = proj.ID
LEFT JOIN sample_properties sp ON sp.sample_raw_id = s.ID
WHERE runset.RUNSET_ID = 'TP002'
GROUP BY s.NAME, s.SAMPLE_ID, ms.SAMPLE_ID, sp.sampling_point, sp.sampling_point_description, sp.line, u.NAME, sp.product_code, sp.product_description, sp.cig_product_code, sp.cig_product_description, sp.spec_group, proj.NAME, rt.LIFE_CYCLE_STATE, peep.ID
HAVING MAX(CASE WHEN pv.VALUE_NUMERIC IS NOT NULL THEN pv.VALUE_NUMERIC END) IS NOT NULL

ORDER BY "Sample ID", "Characteristic";