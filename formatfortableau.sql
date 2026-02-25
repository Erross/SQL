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
    MAX(CASE WHEN p.display_label = 'Spec Group'
             THEN REGEXP_REPLACE(pv.string_value, '^PK-','')
        END) AS spec_group
  FROM hub_owner.cor_class_identity ci
  JOIN hub_owner.cor_object_identity oi
        ON oi.class_identity_id = ci.id
  JOIN hub_owner.cor_property_value pv
        ON pv.object_identity_id = oi.id
  JOIN hub_owner.cor_property p
        ON p.name = pv.property_id
  WHERE ci.table_name = 'sam_sample'
    AND p.display_label IN (
        'Sampling Point',
        'Sampling Point Description',
        'Line',
        'Product Code',
        'Product Description',
        'Cig Product Code',
        'Cig Product Description',
        'Spec Group'
    )
  GROUP BY oi.object_id
),
runset_properties AS (
  SELECT
    oi.object_id AS runset_raw_id,
    ve.name      AS tp_project_plan
  FROM hub_owner.cor_class_identity ci
  JOIN hub_owner.cor_object_identity oi
        ON oi.class_identity_id = ci.id
  JOIN hub_owner.cor_property_value pv
        ON pv.object_identity_id = oi.id
  JOIN hub_owner.cor_property p
        ON p.name = pv.property_id
  JOIN hub_owner.cor_vocab_entry ve
        ON RAWTOHEX(ve.id) = UPPER(REPLACE(pv.string_value, '-', ''))
  WHERE ci.table_name   = 'req_runset'
    AND p.display_label = 'Project Plan'
    AND pv.string_value IS NOT NULL
)

-- =========================
-- MANUAL TEST RESULTS
-- =========================
SELECT
    s.NAME as "Sample Name",
    s.SAMPLE_ID as "Sample ID",
    s.LIFE_CYCLE_STATE as "Sample Status",
    ms.SAMPLE_ID as "Master Sample ID",
    sp.sampling_point as "Sampling point",
    TRIM(REGEXP_REPLACE(
        REPLACE(REPLACE(sp.sampling_point_description, CHR(13), ''), CHR(10), ''),
        '\s*\[[[:digit:]]+\]\s*$',''
    )) as "Sampling point description",
    sp.line as "LINE-1",
    usr.NAME as "Owner",
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
    pv.LAST_UPDATED as "Result entered",
    cs.NAME as "Collaboration Space",
    'MANUAL' as "Result Source",
    uom.description as "UOM",
    rp.tp_project_plan as "Task Plan Project Plan"
FROM hub_owner.COR_PARAMETER_VALUE pv
JOIN hub_owner.COR_PARAMETER p
     ON pv.PARENT_IDENTITY = p.ID
JOIN hub_owner.REQ_TASK_PARAMETER rtp
     ON p.ID = rtp.PARAMETER_ID
JOIN hub_owner.REQ_TASK rt
     ON rtp.TASK_ID = rt.ID
LEFT JOIN hub_owner.REQ_RUNSET runset
     ON rt.RUNSET_ID = runset.ID
LEFT JOIN hub_owner.SAM_SAMPLE s
     ON s.SAMPLE_ID =
        REGEXP_SUBSTR(rt.SAMPLE_LIST, '[^,]+', 1, pv.ITEM_INDEX + 1)
LEFT JOIN hub_owner.SAM_SAMPLE ms
     ON s.MASTER_SAMPLE_ID = ms.ID
LEFT JOIN hub_owner.SEC_USER usr
     ON s.OWNER_ID = usr.ID
LEFT JOIN hub_owner.RES_PROJECT proj
     ON s.PROJECT_ID = proj.ID
LEFT JOIN sample_properties sp
     ON sp.sample_raw_id = s.ID
LEFT JOIN runset_properties rp
     ON rp.runset_raw_id = runset.ID
LEFT JOIN hub_owner.COSPC_OBJECT_IDENTITY coi_sample
     ON coi_sample.OBJECT_ID = s.ID
LEFT JOIN hub_owner.SEC_COLLAB_SPACE cs
     ON cs.ID = coi_sample.COLLABORATIVE_SPACE_ID
LEFT JOIN hub_owner.COR_UNIT uom
     ON pv.UNIT = uom.ID
WHERE pv.VALUE_KEY = 'A'
  AND s.SAMPLE_ID IS NOT NULL
  AND ms.SAMPLE_ID != 'planned'
  AND p.DISPLAY_NAME != 'Sample'
  AND rt.LIFE_CYCLE_STATE IN ('released', 'completed')
  AND p.VALUE_TYPE NOT IN ('Vocabulary')
  AND pv.VALUE_STRING IS NOT NULL
  AND cs.ID = '5FD74EE88C024C2EB908BCE0E176B0E8'

UNION ALL

-- =========================
-- EQUIPMENT MEASUREMENT RESULTS
-- =========================
SELECT
    s.NAME,
    s.SAMPLE_ID,
    s.LIFE_CYCLE_STATE,
    ms.SAMPLE_ID,
    sp.sampling_point,
    TRIM(REGEXP_REPLACE(
        REPLACE(REPLACE(sp.sampling_point_description, CHR(13), ''), CHR(10), ''),
        '\s*\[[[:digit:]]+\]\s*$',''
    )),
    sp.line,
    usr.NAME,
    sp.product_code,
    sp.product_description,
    sp.cig_product_code,
    sp.cig_product_description,
    sp.spec_group,
    proj.NAME,
    runset.RUNSET_ID,
    rt.LIFE_CYCLE_STATE,
    MAX(CASE WHEN pv.VALUE_STRING IS NOT NULL THEN pv.VALUE_STRING END),
    NULL,
    TO_CHAR(MAX(CASE WHEN pv.VALUE_NUMERIC IS NOT NULL THEN pv.VALUE_NUMERIC END)),
    TO_CHAR(MAX(CASE WHEN pv.VALUE_NUMERIC IS NOT NULL THEN pv.VALUE_NUMERIC END)),
    MAX(pv.LAST_UPDATED),
    cs.NAME,
    'EQUIPMENT',
    MAX(uom.description),
    rp.tp_project_plan
FROM hub_owner.PEX_PROC_EXEC pe
JOIN hub_owner.REQ_TASK rt
     ON rt.WORK_ITEM LIKE '%' || LOWER(
            SUBSTR(RAWTOHEX(pe.ID),1,8)  ||'-'||
            SUBSTR(RAWTOHEX(pe.ID),9,4)  ||'-'||
            SUBSTR(RAWTOHEX(pe.ID),13,4) ||'-'||
            SUBSTR(RAWTOHEX(pe.ID),17,4) ||'-'||
            SUBSTR(RAWTOHEX(pe.ID),21,12)
        ) || '%'
JOIN hub_owner.PEX_PROC_ELEM_EXEC pee
     ON pee.PARENT_ID = pe.ID
JOIN hub_owner.RES_RETRIEVAL_CONTEXT ctx
     ON ctx.CONTEXT =
        'urn:pexelement:' ||
        LOWER(
            SUBSTR(RAWTOHEX(pee.ID),1,8)||'-'||
            SUBSTR(RAWTOHEX(pee.ID),9,4)||'-'||
            SUBSTR(RAWTOHEX(pee.ID),13,4)||'-'||
            SUBSTR(RAWTOHEX(pee.ID),17,4)||'-'||
            SUBSTR(RAWTOHEX(pee.ID),21,12)
        )
JOIN hub_owner.RES_MEASUREMENTSAMPLE meas_s
     ON meas_s.CONTEXT_ID = ctx.ID
JOIN hub_owner.RES_MEASUREMENT m
     ON m.ID = meas_s.MEASUREMENT_ID
JOIN hub_owner.SAM_SAMPLE s
     ON s.SAMPLE_ID = meas_s.SAMPLE_ID
JOIN hub_owner.PEX_PROC_ELEM_EXEC_PARAM peep
     ON peep.PARENT_ID = pee.ID
JOIN hub_owner.COR_PARAMETER_VALUE pv
     ON pv.PARENT_IDENTITY = peep.ID
     AND pv.ITEM_INDEX = meas_s.ROW_INDEX
LEFT JOIN hub_owner.SAM_SAMPLE ms
     ON s.MASTER_SAMPLE_ID = ms.ID
LEFT JOIN hub_owner.SEC_USER usr
     ON s.OWNER_ID = usr.ID
LEFT JOIN hub_owner.RES_PROJECT proj
     ON s.PROJECT_ID = proj.ID
LEFT JOIN sample_properties sp
     ON sp.sample_raw_id = s.ID
LEFT JOIN hub_owner.REQ_RUNSET runset
     ON rt.RUNSET_ID = runset.ID
LEFT JOIN runset_properties rp
     ON rp.runset_raw_id = runset.ID
LEFT JOIN hub_owner.COSPC_OBJECT_IDENTITY coi_sample
     ON coi_sample.OBJECT_ID = s.ID
LEFT JOIN hub_owner.SEC_COLLAB_SPACE cs
     ON cs.ID = coi_sample.COLLABORATIVE_SPACE_ID
LEFT JOIN hub_owner.COR_UNIT uom
     ON pv.UNIT = uom.ID
WHERE s.SAMPLE_ID IS NOT NULL
  AND rt.LIFE_CYCLE_STATE IN ('released', 'completed')
  AND cs.ID = '5FD74EE88C024C2EB908BCE0E176B0E8'
  AND ms.SAMPLE_ID != 'planned'
  AND pv.VALUE_STRING != 'sample'
GROUP BY
    s.NAME, s.SAMPLE_ID, s.LIFE_CYCLE_STATE, rp.tp_project_plan,
    ms.SAMPLE_ID, sp.sampling_point,
    sp.sampling_point_description, sp.line,
    usr.NAME, sp.product_code, sp.product_description,
    sp.cig_product_code, sp.cig_product_description,
    sp.spec_group, proj.NAME, runset.RUNSET_ID,
    rt.LIFE_CYCLE_STATE, cs.NAME, peep.ID
HAVING MAX(CASE WHEN pv.VALUE_NUMERIC IS NOT NULL
                THEN pv.VALUE_NUMERIC END) IS NOT NULL;

                SELECT 
    rt.TASK_ID,
    rt.LIFE_CYCLE_STATE as task_status,
    rt.SAMPLE_LIST,
    runset.RUNSET_ID,
    runset.LIFE_CYCLE_STATE as runset_status
FROM hub_owner.REQ_TASK rt
LEFT JOIN hub_owner.REQ_RUNSET runset ON rt.RUNSET_ID = runset.ID
WHERE INSTR(','||rt.SAMPLE_LIST||',', ',S001053,') > 0;

SELECT 
    rt.TASK_ID,
    rt.LIFE_CYCLE_STATE as task_status,
    rt.RUNSET_ID,
    runset.RUNSET_ID as runset_id,
    runset.LIFE_CYCLE_STATE as runset_status
FROM hub_owner.REQ_TASK rt
LEFT JOIN hub_owner.REQ_RUNSET runset ON rt.RUNSET_ID = runset.ID
WHERE INSTR(','||rt.SAMPLE_LIST||',', ',S001053,') > 0
ORDER BY rt.LAST_UPDATED DESC;


SELECT 
    runset.RUNSET_ID,
    runset.LIFE_CYCLE_STATE,
    runset.NAME
FROM hub_owner.REQ_RUNSET runset
JOIN hub_owner.REQ_TASK rt ON rt.RUNSET_ID = runset.ID
WHERE INSTR(','||rt.SAMPLE_LIST||',', ',S001053,') > 0;

SELECT 
    se.EVENT_TYPE,
    se.LIFE_CYCLE_STATE,
    se.EVENT_TIME,
    se.EVENT_DATA,
    se.EVENT_CONTEXT
FROM hub_owner.SAM_SAMPLE_EVENT se
JOIN hub_owner.SAM_SAMPLE s ON s.ID = se.SAMPLE_ID
WHERE s.SAMPLE_ID = 'S001053'
ORDER BY se.EVENT_TIME DESC;

--audit

SELECT 
    a.REVTYPE,
    a.LIFE_CYCLE_STATE,
    a.LAST_UPDATED,
    a.SAMPLE_ID
FROM hub_owner.AUD_SAM_SAMPLE a
JOIN hub_owner.SAM_SAMPLE s ON s.ID = a.SAMPLE_ID
WHERE s.SAMPLE_ID = 'S001053'
ORDER BY a.LAST_UPDATED DESC;

--extended?

SELECT 
    p.display_label,
    p.name,
    pv.string_value,
    pv.number_value
FROM hub_owner.cor_object_identity oi
JOIN hub_owner.cor_property_value pv ON pv.object_identity_id = oi.id
JOIN hub_owner.cor_property p ON p.name = pv.property_id
JOIN hub_owner.cor_class_identity ci ON ci.id = oi.class_identity_id
JOIN hub_owner.SAM_SAMPLE s ON s.ID = oi.object_id
WHERE ci.table_name = 'sam_sample'
  AND s.SAMPLE_ID = 'S001053';

  SELECT DISTINCT
    pv.VALUE_KEY,
    pv.VALUE_STRING,
    pv.VALUE_NUMERIC,
    pv.ITEM_INDEX,
    p.DISPLAY_NAME
FROM hub_owner.COR_PARAMETER_VALUE pv
JOIN hub_owner.COR_PARAMETER p ON pv.PARENT_IDENTITY = p.ID
JOIN hub_owner.REQ_TASK_PARAMETER rtp ON p.ID = rtp.PARAMETER_ID
JOIN hub_owner.REQ_TASK rt ON rtp.TASK_ID = rt.ID
WHERE INSTR(','||rt.SAMPLE_LIST||',', ',S001053,') > 0
ORDER BY pv.ITEM_INDEX, pv.VALUE_KEY;

SELECT 
    rt.SAMPLE_LIST,
    pee.ITEM_STATES,
    pee.STATE
FROM hub_owner.REQ_TASK rt
JOIN hub_owner.PEX_PROC_EXEC pe
     ON rt.WORK_ITEM LIKE '%' || LOWER(
            SUBSTR(RAWTOHEX(pe.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe.ID),9,4)||'-'||
            SUBSTR(RAWTOHEX(pe.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe.ID),17,4)||'-'||
            SUBSTR(RAWTOHEX(pe.ID),21,12)) || '%'
JOIN hub_owner.PEX_PROC_ELEM_EXEC pee ON pee.PARENT_ID = pe.ID
WHERE INSTR(','||rt.SAMPLE_LIST||',', ',S001053,') > 0;
SELECT 
    pee.SOURCE_POSITION,
    pee.ITEM_STATES,
    LENGTH(pee.ITEM_STATES) as states_length,
    rt.SAMPLE_LIST,
    -- how many samples in the list
    LENGTH(rt.SAMPLE_LIST) - LENGTH(REPLACE(rt.SAMPLE_LIST,',','')) + 1 as sample_count
FROM hub_owner.REQ_TASK rt
JOIN hub_owner.PEX_PROC_EXEC pe
     ON rt.WORK_ITEM LIKE '%' || LOWER(
            SUBSTR(RAWTOHEX(pe.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe.ID),9,4)||'-'||
            SUBSTR(RAWTOHEX(pe.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe.ID),17,4)||'-'||
            SUBSTR(RAWTOHEX(pe.ID),21,12)) || '%'
JOIN hub_owner.PEX_PROC_ELEM_EXEC pee ON pee.PARENT_ID = pe.ID
WHERE INSTR(','||rt.SAMPLE_LIST||',', ',S001053,') > 0
  AND pee.ITEM_STATES IS NOT NULL
  AND pee.ITEM_STATES != '__________';

  SELECT 
    pee.SOURCE_POSITION,
    pee.PROCESS_NUMBER,
    pee.STATE,
    pee.ITEM_STATES
FROM hub_owner.REQ_TASK rt
JOIN hub_owner.PEX_PROC_EXEC pe
     ON rt.WORK_ITEM LIKE '%' || LOWER(
            SUBSTR(RAWTOHEX(pe.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe.ID),9,4)||'-'||
            SUBSTR(RAWTOHEX(pe.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe.ID),17,4)||'-'||
            SUBSTR(RAWTOHEX(pe.ID),21,12)) || '%'
JOIN hub_owner.PEX_PROC_ELEM_EXEC pee ON pee.PARENT_ID = pe.ID
WHERE rt.TASK_ID = 'T313'
  AND pee.ITEM_STATES IS NOT NULL
  AND pee.ITEM_STATES != '__________'
ORDER BY pee.SOURCE_POSITION;

--NEW CRAZY APPROACH

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
    MAX(CASE WHEN p.display_label = 'Spec Group'
             THEN REGEXP_REPLACE(pv.string_value, '^PK-','')
        END) AS spec_group
  FROM hub_owner.cor_class_identity ci
  JOIN hub_owner.cor_object_identity oi
        ON oi.class_identity_id = ci.id
  JOIN hub_owner.cor_property_value pv
        ON pv.object_identity_id = oi.id
  JOIN hub_owner.cor_property p
        ON p.name = pv.property_id
  WHERE ci.table_name = 'sam_sample'
    AND p.display_label IN (
        'Sampling Point',
        'Sampling Point Description',
        'Line',
        'Product Code',
        'Product Description',
        'Cig Product Code',
        'Cig Product Description',
        'Spec Group'
    )
  GROUP BY oi.object_id
),
runset_properties AS (
  SELECT
    oi.object_id AS runset_raw_id,
    ve.name      AS tp_project_plan
  FROM hub_owner.cor_class_identity ci
  JOIN hub_owner.cor_object_identity oi
        ON oi.class_identity_id = ci.id
  JOIN hub_owner.cor_property_value pv
        ON pv.object_identity_id = oi.id
  JOIN hub_owner.cor_property p
        ON p.name = pv.property_id
  JOIN hub_owner.cor_vocab_entry ve
        ON RAWTOHEX(ve.id) = UPPER(REPLACE(pv.string_value, '-', ''))
  WHERE ci.table_name   = 'req_runset'
    AND p.display_label = 'Project Plan'
    AND pv.string_value IS NOT NULL
)

-- =========================
-- MANUAL TEST RESULTS
-- =========================
SELECT
    s.NAME as "Sample Name",
    s.SAMPLE_ID as "Sample ID",
    (SELECT CASE MAX(SUBSTR(pee2.ITEM_STATES, (pv.ITEM_INDEX * 2) + 1, 2))
                 WHEN 'XX' THEN 'abandoned'
                 WHEN 'DD' THEN 'completed'
                 ELSE s.LIFE_CYCLE_STATE
            END
     FROM hub_owner.PEX_PROC_EXEC pe2
     JOIN hub_owner.PEX_PROC_ELEM_EXEC pee2 ON pee2.PARENT_ID = pe2.ID
     WHERE rt.WORK_ITEM LIKE '%' || LOWER(
                SUBSTR(RAWTOHEX(pe2.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe2.ID),9,4)||'-'||
                SUBSTR(RAWTOHEX(pe2.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe2.ID),17,4)||'-'||
                SUBSTR(RAWTOHEX(pe2.ID),21,12)) || '%'
       AND pee2.ITEM_STATES IS NOT NULL
    ) as "Sample Status",
    ms.SAMPLE_ID as "Master Sample ID",
    sp.sampling_point as "Sampling point",
    TRIM(REGEXP_REPLACE(
        REPLACE(REPLACE(sp.sampling_point_description, CHR(13), ''), CHR(10), ''),
        '\s*\[[[:digit:]]+\]\s*$',''
    )) as "Sampling point description",
    sp.line as "LINE-1",
    usr.NAME as "Owner",
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
    pv.LAST_UPDATED as "Result entered",
    cs.NAME as "Collaboration Space",
    'MANUAL' as "Result Source",
    uom.description as "UOM",
    rp.tp_project_plan as "Task Plan Project Plan"
FROM hub_owner.COR_PARAMETER_VALUE pv
JOIN hub_owner.COR_PARAMETER p
     ON pv.PARENT_IDENTITY = p.ID
JOIN hub_owner.REQ_TASK_PARAMETER rtp
     ON p.ID = rtp.PARAMETER_ID
JOIN hub_owner.REQ_TASK rt
     ON rtp.TASK_ID = rt.ID
LEFT JOIN hub_owner.REQ_RUNSET runset
     ON rt.RUNSET_ID = runset.ID
LEFT JOIN hub_owner.SAM_SAMPLE s
     ON s.SAMPLE_ID =
        REGEXP_SUBSTR(rt.SAMPLE_LIST, '[^,]+', 1, pv.ITEM_INDEX + 1)
LEFT JOIN hub_owner.SAM_SAMPLE ms
     ON s.MASTER_SAMPLE_ID = ms.ID
LEFT JOIN hub_owner.SEC_USER usr
     ON s.OWNER_ID = usr.ID
LEFT JOIN hub_owner.RES_PROJECT proj
     ON s.PROJECT_ID = proj.ID
LEFT JOIN sample_properties sp
     ON sp.sample_raw_id = s.ID
LEFT JOIN runset_properties rp
     ON rp.runset_raw_id = runset.ID
LEFT JOIN hub_owner.COSPC_OBJECT_IDENTITY coi_sample
     ON coi_sample.OBJECT_ID = s.ID
LEFT JOIN hub_owner.SEC_COLLAB_SPACE cs
     ON cs.ID = coi_sample.COLLABORATIVE_SPACE_ID
LEFT JOIN hub_owner.COR_UNIT uom
     ON pv.UNIT = uom.ID
WHERE pv.VALUE_KEY = 'A'
  AND s.SAMPLE_ID IS NOT NULL
  AND ms.SAMPLE_ID != 'planned'
  AND p.DISPLAY_NAME != 'Sample'
  AND rt.LIFE_CYCLE_STATE IN ('released', 'completed')
  AND p.VALUE_TYPE NOT IN ('Vocabulary')
  AND pv.VALUE_STRING IS NOT NULL
  AND cs.ID = '5FD74EE88C024C2EB908BCE0E176B0E8'

UNION ALL

-- =========================
-- EQUIPMENT MEASUREMENT RESULTS
-- =========================
SELECT
    s.NAME,
    s.SAMPLE_ID,
    CASE MAX(SUBSTR(pee.ITEM_STATES, (meas_s.ROW_INDEX * 2) + 1, 2))
        WHEN 'XX' THEN 'abandoned'
        WHEN 'DD' THEN 'completed'
        ELSE MAX(s.LIFE_CYCLE_STATE)
    END,
    ms.SAMPLE_ID,
    sp.sampling_point,
    TRIM(REGEXP_REPLACE(
        REPLACE(REPLACE(sp.sampling_point_description, CHR(13), ''), CHR(10), ''),
        '\s*\[[[:digit:]]+\]\s*$',''
    )),
    sp.line,
    usr.NAME,
    sp.product_code,
    sp.product_description,
    sp.cig_product_code,
    sp.cig_product_description,
    sp.spec_group,
    proj.NAME,
    runset.RUNSET_ID,
    rt.LIFE_CYCLE_STATE,
    MAX(CASE WHEN pv.VALUE_STRING IS NOT NULL THEN pv.VALUE_STRING END),
    NULL,
    TO_CHAR(MAX(CASE WHEN pv.VALUE_NUMERIC IS NOT NULL THEN pv.VALUE_NUMERIC END)),
    TO_CHAR(MAX(CASE WHEN pv.VALUE_NUMERIC IS NOT NULL THEN pv.VALUE_NUMERIC END)),
    MAX(pv.LAST_UPDATED),
    cs.NAME,
    'EQUIPMENT',
    MAX(uom.description),
    rp.tp_project_plan
FROM hub_owner.PEX_PROC_EXEC pe
JOIN hub_owner.REQ_TASK rt
     ON rt.WORK_ITEM LIKE '%' || LOWER(
            SUBSTR(RAWTOHEX(pe.ID),1,8)  ||'-'||
            SUBSTR(RAWTOHEX(pe.ID),9,4)  ||'-'||
            SUBSTR(RAWTOHEX(pe.ID),13,4) ||'-'||
            SUBSTR(RAWTOHEX(pe.ID),17,4) ||'-'||
            SUBSTR(RAWTOHEX(pe.ID),21,12)
        ) || '%'
JOIN hub_owner.PEX_PROC_ELEM_EXEC pee
     ON pee.PARENT_ID = pe.ID
JOIN hub_owner.RES_RETRIEVAL_CONTEXT ctx
     ON ctx.CONTEXT =
        'urn:pexelement:' ||
        LOWER(
            SUBSTR(RAWTOHEX(pee.ID),1,8)||'-'||
            SUBSTR(RAWTOHEX(pee.ID),9,4)||'-'||
            SUBSTR(RAWTOHEX(pee.ID),13,4)||'-'||
            SUBSTR(RAWTOHEX(pee.ID),17,4)||'-'||
            SUBSTR(RAWTOHEX(pee.ID),21,12)
        )
JOIN hub_owner.RES_MEASUREMENTSAMPLE meas_s
     ON meas_s.CONTEXT_ID = ctx.ID
JOIN hub_owner.RES_MEASUREMENT m
     ON m.ID = meas_s.MEASUREMENT_ID
JOIN hub_owner.SAM_SAMPLE s
     ON s.SAMPLE_ID = meas_s.SAMPLE_ID
JOIN hub_owner.PEX_PROC_ELEM_EXEC_PARAM peep
     ON peep.PARENT_ID = pee.ID
JOIN hub_owner.COR_PARAMETER_VALUE pv
     ON pv.PARENT_IDENTITY = peep.ID
     AND pv.ITEM_INDEX = meas_s.ROW_INDEX
LEFT JOIN hub_owner.SAM_SAMPLE ms
     ON s.MASTER_SAMPLE_ID = ms.ID
LEFT JOIN hub_owner.SEC_USER usr
     ON s.OWNER_ID = usr.ID
LEFT JOIN hub_owner.RES_PROJECT proj
     ON s.PROJECT_ID = proj.ID
LEFT JOIN sample_properties sp
     ON sp.sample_raw_id = s.ID
LEFT JOIN hub_owner.REQ_RUNSET runset
     ON rt.RUNSET_ID = runset.ID
LEFT JOIN runset_properties rp
     ON rp.runset_raw_id = runset.ID
LEFT JOIN hub_owner.COSPC_OBJECT_IDENTITY coi_sample
     ON coi_sample.OBJECT_ID = s.ID
LEFT JOIN hub_owner.SEC_COLLAB_SPACE cs
     ON cs.ID = coi_sample.COLLABORATIVE_SPACE_ID
LEFT JOIN hub_owner.COR_UNIT uom
     ON pv.UNIT = uom.ID
WHERE s.SAMPLE_ID IS NOT NULL
  AND rt.LIFE_CYCLE_STATE IN ('released', 'completed')
  AND cs.ID = '5FD74EE88C024C2EB908BCE0E176B0E8'
  AND ms.SAMPLE_ID != 'planned'
  AND pv.VALUE_STRING != 'sample'
GROUP BY
    s.NAME, s.SAMPLE_ID, s.LIFE_CYCLE_STATE, rp.tp_project_plan,
    ms.SAMPLE_ID, sp.sampling_point,
    sp.sampling_point_description, sp.line,
    usr.NAME, sp.product_code, sp.product_description,
    sp.cig_product_code, sp.cig_product_description,
    sp.spec_group, proj.NAME, runset.RUNSET_ID,
    rt.LIFE_CYCLE_STATE, cs.NAME, peep.ID
HAVING MAX(CASE WHEN pv.VALUE_NUMERIC IS NOT NULL
                THEN pv.VALUE_NUMERIC END) IS NOT NULL