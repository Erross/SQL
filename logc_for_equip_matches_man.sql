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

),
sample_status AS (
  -- Computes abandoned/completed per SAMPLE_ID for use by the equipment section.
  -- Uses ROW_NUMBER() to get each sample's 1-based absolute batch position
  -- (ordered by SAMPLE_ID ASC within the PEX_PROC_EXEC) - identical result to
  -- the COUNT(smaller SAMPLE_IDs)+1 approach but valid inside a WITH clause.
  -- No correlated subqueries, which Oracle prohibits inside CTE definitions.
  SELECT
      rn.SAMPLE_ID,
      CASE
          WHEN MAX(CASE WHEN UPPER(SUBSTR(pee.ITEM_STATES, rn.pos, 1)) = 'X' THEN 1 END) = 1 THEN 'abandoned'
          WHEN MAX(CASE WHEN UPPER(SUBSTR(pee.ITEM_STATES, rn.pos, 1)) = 'D' THEN 1 END) = 1 THEN 'completed'
          ELSE NULL
      END AS sample_status
  FROM (
      SELECT
          pe.ID   AS pe_id,
          s.SAMPLE_ID,
          ROW_NUMBER() OVER (PARTITION BY pe.ID ORDER BY s.SAMPLE_ID) AS pos
      FROM hub_owner.PEX_PROC_EXEC pe
      JOIN hub_owner.REQ_TASK rt
           ON rt.WORK_ITEM LIKE '%' || LOWER(
                  SUBSTR(RAWTOHEX(pe.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe.ID),9,4)||'-'||
                  SUBSTR(RAWTOHEX(pe.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe.ID),17,4)||'-'||
                  SUBSTR(RAWTOHEX(pe.ID),21,12)) || '%'
           AND rt.LIFE_CYCLE_STATE IN ('released', 'completed')
      JOIN hub_owner.SAM_SAMPLE s
           ON INSTR(','||rt.SAMPLE_LIST||',', ','||s.SAMPLE_ID||',') > 0
  ) rn
  JOIN hub_owner.PEX_PROC_ELEM_EXEC pee ON pee.PARENT_ID = rn.pe_id
  WHERE pee.ITEM_STATES IS NOT NULL
  GROUP BY rn.SAMPLE_ID
)

-- =========================
-- MANUAL TEST RESULTS
-- 1  Sample Name
-- 2  Sample ID
-- 3  Sample Status
-- 4  Master Sample ID
-- 5  Sampling point
-- 6  Sampling point description
-- 7  LINE-1
-- 8  Owner
-- 9  Product Code
-- 10 Product Description
-- 11 CIG_PRODUCT_CODE
-- 12 CIG_PRODUCT_DESCRIPTION
-- 13 Spec_Group
-- 14 Task Plan Project
-- 15 Task Plan ID
-- 16 Task Status
-- 17 Characteristic
-- 18 Compose Details
-- 19 Result
-- 20 Formatted result
-- 21 Result entered
-- 22 Collaboration Space
-- 23 Result Source
-- 24 UOM
-- 25 Item States (Debug)
-- 26 Task Plan Project Plan
-- =========================
SELECT
    s.NAME                      AS "Sample Name",
    s.SAMPLE_ID                 AS "Sample ID",
    -- Sample Status: scan ALL pee rows for this PEX_PROC_EXEC.
    -- X in ANY row at this sample's absolute batch position (= COUNT of co-run
    -- samples with smaller SAMPLE_ID + 1) → abandoned. D (no X) → completed.
    -- SOURCE_POSITION of the acceptance row varies by instrument, so no SP filter.
    (SELECT CASE
                WHEN MAX(CASE WHEN
                         UPPER(SUBSTR(pee2.ITEM_STATES,
                             (SELECT COUNT(*) + 1
                              FROM hub_owner.REQ_TASK rt2
                              JOIN hub_owner.SAM_SAMPLE s2
                                   ON INSTR(','||rt2.SAMPLE_LIST||',', ','||s2.SAMPLE_ID||',') > 0
                              WHERE rt2.WORK_ITEM LIKE '%' || LOWER(
                                          SUBSTR(RAWTOHEX(pe2.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe2.ID),9,4)||'-'||
                                          SUBSTR(RAWTOHEX(pe2.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe2.ID),17,4)||'-'||
                                          SUBSTR(RAWTOHEX(pe2.ID),21,12)) || '%'
                                AND s2.SAMPLE_ID < s.SAMPLE_ID),
                             1)) = 'X' THEN 1 END) = 1 THEN 'abandoned'
                WHEN MAX(CASE WHEN
                         UPPER(SUBSTR(pee2.ITEM_STATES,
                             (SELECT COUNT(*) + 1
                              FROM hub_owner.REQ_TASK rt2
                              JOIN hub_owner.SAM_SAMPLE s2
                                   ON INSTR(','||rt2.SAMPLE_LIST||',', ','||s2.SAMPLE_ID||',') > 0
                              WHERE rt2.WORK_ITEM LIKE '%' || LOWER(
                                          SUBSTR(RAWTOHEX(pe2.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe2.ID),9,4)||'-'||
                                          SUBSTR(RAWTOHEX(pe2.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe2.ID),17,4)||'-'||
                                          SUBSTR(RAWTOHEX(pe2.ID),21,12)) || '%'
                                AND s2.SAMPLE_ID < s.SAMPLE_ID),
                             1)) = 'D' THEN 1 END) = 1 THEN 'completed'
                ELSE s.LIFE_CYCLE_STATE
            END
     FROM hub_owner.PEX_PROC_EXEC pe2
     JOIN hub_owner.PEX_PROC_ELEM_EXEC pee2 ON pee2.PARENT_ID = pe2.ID
     WHERE rt.WORK_ITEM LIKE '%' || LOWER(
                SUBSTR(RAWTOHEX(pe2.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe2.ID),9,4)||'-'||
                SUBSTR(RAWTOHEX(pe2.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe2.ID),17,4)||'-'||
                SUBSTR(RAWTOHEX(pe2.ID),21,12)) || '%'
       AND pee2.ITEM_STATES IS NOT NULL
    )                           AS "Sample Status",
    ms.SAMPLE_ID                AS "Master Sample ID",
    sp.sampling_point           AS "Sampling point",
    TRIM(REGEXP_REPLACE(
        REPLACE(REPLACE(sp.sampling_point_description, CHR(13), ''), CHR(10), ''),
        '\s*\[[[:digit:]]+\]\s*$',''
    ))                          AS "Sampling point description",
    sp.line                     AS "LINE-1",
    usr.NAME                    AS "Owner",
    sp.product_code             AS "Product Code",
    sp.product_description      AS "Product Description",
    sp.cig_product_code         AS "CIG_PRODUCT_CODE",
    sp.cig_product_description  AS "CIG_PRODUCT_DESCRIPTION",
    sp.spec_group               AS "Spec_Group",
    proj.NAME                   AS "Task Plan Project",
    runset.RUNSET_ID            AS "Task Plan ID",
    rt.LIFE_CYCLE_STATE         AS "Task Status",
    p.DISPLAY_NAME              AS "Characteristic",
    pv.INTERPRETATION           AS "Compose Details",
    pv.VALUE_STRING             AS "Result",
    pv.VALUE_TEXT               AS "Formatted result",
    pv.LAST_UPDATED             AS "Result entered",
    cs.NAME                     AS "Collaboration Space",
    'MANUAL'                    AS "Result Source",
    uom.description             AS "UOM",
    (SELECT MAX(pee2.SOURCE_POSITION||':'||pee2.ITEM_STATES)
     FROM hub_owner.PEX_PROC_EXEC pe2
     JOIN hub_owner.PEX_PROC_ELEM_EXEC pee2 ON pee2.PARENT_ID = pe2.ID
     WHERE rt.WORK_ITEM LIKE '%' || LOWER(
                SUBSTR(RAWTOHEX(pe2.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe2.ID),9,4)||'-'||
                SUBSTR(RAWTOHEX(pe2.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe2.ID),17,4)||'-'||
                SUBSTR(RAWTOHEX(pe2.ID),21,12)) || '%'
       AND pee2.ITEM_STATES IS NOT NULL
       AND REGEXP_LIKE(pee2.ITEM_STATES, '[XD]')
    )                           AS "Item States (Debug)",
    rp.tp_project_plan          AS "Task Plan Project Plan"
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
     ON runset.PROJECT_ID = proj.ID
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
-- Column order matches manual exactly
-- =========================
SELECT
    s.NAME,                                                                          -- 1  Sample Name
    s.SAMPLE_ID,                                                                     -- 2  Sample ID
    COALESCE(ss.sample_status, s.LIFE_CYCLE_STATE),                               -- 3  Sample Status
    ms.SAMPLE_ID,                                                                    -- 4  Master Sample ID
    sp.sampling_point,                                                               -- 5  Sampling point
    TRIM(REGEXP_REPLACE(                                                             -- 6  Sampling point description
        REPLACE(REPLACE(sp.sampling_point_description, CHR(13), ''), CHR(10), ''),
        '\s*\[[[:digit:]]+\]\s*$',''
    )),
    sp.line,                                                                         -- 7  LINE-1
    usr.NAME,                                                                        -- 8  Owner
    sp.product_code,                                                                 -- 9  Product Code
    sp.product_description,                                                          -- 10 Product Description
    sp.cig_product_code,                                                             -- 11 CIG_PRODUCT_CODE
    sp.cig_product_description,                                                      -- 12 CIG_PRODUCT_DESCRIPTION
    sp.spec_group,                                                                   -- 13 Spec_Group
    proj.NAME,                                                                       -- 14 Task Plan Project
    runset.RUNSET_ID,                                                                -- 15 Task Plan ID
    rt.LIFE_CYCLE_STATE,                                                             -- 16 Task Status
    MAX(CASE WHEN pv.VALUE_STRING IS NOT NULL THEN pv.VALUE_STRING END),             -- 17 Characteristic
    NULL,                                                                            -- 18 Compose Details
    TO_CHAR(MAX(CASE WHEN pv.VALUE_NUMERIC IS NOT NULL THEN pv.VALUE_NUMERIC END)),  -- 19 Result
    TO_CHAR(MAX(CASE WHEN pv.VALUE_NUMERIC IS NOT NULL THEN pv.VALUE_NUMERIC END)),  -- 20 Formatted result
    MAX(pv.LAST_UPDATED),                                                            -- 21 Result entered
    cs.NAME,                                                                         -- 22 Collaboration Space
    'EQUIPMENT',                                                                     -- 23 Result Source
    MAX(uom.description),                                                            -- 24 UOM
    (SELECT MAX(pee2.SOURCE_POSITION||':'||pee2.ITEM_STATES)                        -- 25 Item States (Debug)
     FROM hub_owner.PEX_PROC_ELEM_EXEC pee2
     WHERE pee2.PARENT_ID = pe.ID
       AND pee2.ITEM_STATES IS NOT NULL
       AND REGEXP_LIKE(pee2.ITEM_STATES, '[XD]')
    ),
    rp.tp_project_plan                                                               -- 26 Task Plan Project Plan
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
LEFT JOIN sample_properties sp
     ON sp.sample_raw_id = s.ID
LEFT JOIN hub_owner.REQ_RUNSET runset
     ON rt.RUNSET_ID = runset.ID
LEFT JOIN hub_owner.RES_PROJECT proj
     ON runset.PROJECT_ID = proj.ID
LEFT JOIN runset_properties rp
     ON rp.runset_raw_id = runset.ID
LEFT JOIN hub_owner.COSPC_OBJECT_IDENTITY coi_sample
     ON coi_sample.OBJECT_ID = s.ID
LEFT JOIN hub_owner.SEC_COLLAB_SPACE cs
     ON cs.ID = coi_sample.COLLABORATIVE_SPACE_ID
LEFT JOIN hub_owner.COR_UNIT uom
     ON pv.UNIT = uom.ID
LEFT JOIN sample_status ss
     ON ss.SAMPLE_ID = s.SAMPLE_ID
WHERE s.SAMPLE_ID IS NOT NULL
  AND rt.LIFE_CYCLE_STATE IN ('released', 'completed')
  AND cs.ID = '5FD74EE88C024C2EB908BCE0E176B0E8'
  AND ms.SAMPLE_ID != 'planned'
  AND pv.VALUE_STRING != 'sample'
GROUP BY
    s.NAME, s.SAMPLE_ID, s.LIFE_CYCLE_STATE,
    ms.SAMPLE_ID, sp.sampling_point,
    sp.sampling_point_description, sp.line,
    usr.NAME, sp.product_code, sp.product_description,
    sp.cig_product_code, sp.cig_product_description,
    sp.spec_group, proj.NAME, runset.RUNSET_ID,
    rt.LIFE_CYCLE_STATE, rt.SAMPLE_LIST, cs.NAME, peep.ID,
    pe.ID, meas_s.ROW_INDEX, rp.tp_project_plan, ss.sample_status
HAVING MAX(CASE WHEN pv.VALUE_NUMERIC IS NOT NULL
                THEN pv.VALUE_NUMERIC END) IS NOT NULL;

                ----- without CTE?

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
-- 1  Sample Name
-- 2  Sample ID
-- 3  Sample Status
-- 4  Master Sample ID
-- 5  Sampling point
-- 6  Sampling point description
-- 7  LINE-1
-- 8  Owner
-- 9  Product Code
-- 10 Product Description
-- 11 CIG_PRODUCT_CODE
-- 12 CIG_PRODUCT_DESCRIPTION
-- 13 Spec_Group
-- 14 Task Plan Project
-- 15 Task Plan ID
-- 16 Task Status
-- 17 Characteristic
-- 18 Compose Details
-- 19 Result
-- 20 Formatted result
-- 21 Result entered
-- 22 Collaboration Space
-- 23 Result Source
-- 24 UOM
-- 25 Item States (Debug)
-- 26 Task Plan Project Plan
-- =========================
SELECT
    s.NAME                      AS "Sample Name",
    s.SAMPLE_ID                 AS "Sample ID",
    -- Sample Status: scan ALL pee rows for this PEX_PROC_EXEC.
    -- X in ANY row at this sample's absolute batch position (= COUNT of co-run
    -- samples with smaller SAMPLE_ID + 1) → abandoned. D (no X) → completed.
    -- SOURCE_POSITION of the acceptance row varies by instrument, so no SP filter.
    (SELECT CASE
                WHEN MAX(CASE WHEN
                         UPPER(SUBSTR(pee2.ITEM_STATES,
                             (SELECT COUNT(*) + 1
                              FROM hub_owner.REQ_TASK rt2
                              JOIN hub_owner.SAM_SAMPLE s2
                                   ON INSTR(','||rt2.SAMPLE_LIST||',', ','||s2.SAMPLE_ID||',') > 0
                              WHERE rt2.WORK_ITEM LIKE '%' || LOWER(
                                          SUBSTR(RAWTOHEX(pe2.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe2.ID),9,4)||'-'||
                                          SUBSTR(RAWTOHEX(pe2.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe2.ID),17,4)||'-'||
                                          SUBSTR(RAWTOHEX(pe2.ID),21,12)) || '%'
                                AND s2.SAMPLE_ID < s.SAMPLE_ID),
                             1)) = 'X' THEN 1 END) = 1 THEN 'abandoned'
                WHEN MAX(CASE WHEN
                         UPPER(SUBSTR(pee2.ITEM_STATES,
                             (SELECT COUNT(*) + 1
                              FROM hub_owner.REQ_TASK rt2
                              JOIN hub_owner.SAM_SAMPLE s2
                                   ON INSTR(','||rt2.SAMPLE_LIST||',', ','||s2.SAMPLE_ID||',') > 0
                              WHERE rt2.WORK_ITEM LIKE '%' || LOWER(
                                          SUBSTR(RAWTOHEX(pe2.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe2.ID),9,4)||'-'||
                                          SUBSTR(RAWTOHEX(pe2.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe2.ID),17,4)||'-'||
                                          SUBSTR(RAWTOHEX(pe2.ID),21,12)) || '%'
                                AND s2.SAMPLE_ID < s.SAMPLE_ID),
                             1)) = 'D' THEN 1 END) = 1 THEN 'completed'
                ELSE s.LIFE_CYCLE_STATE
            END
     FROM hub_owner.PEX_PROC_EXEC pe2
     JOIN hub_owner.PEX_PROC_ELEM_EXEC pee2 ON pee2.PARENT_ID = pe2.ID
     WHERE rt.WORK_ITEM LIKE '%' || LOWER(
                SUBSTR(RAWTOHEX(pe2.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe2.ID),9,4)||'-'||
                SUBSTR(RAWTOHEX(pe2.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe2.ID),17,4)||'-'||
                SUBSTR(RAWTOHEX(pe2.ID),21,12)) || '%'
       AND pee2.ITEM_STATES IS NOT NULL
    )                           AS "Sample Status",
    ms.SAMPLE_ID                AS "Master Sample ID",
    sp.sampling_point           AS "Sampling point",
    TRIM(REGEXP_REPLACE(
        REPLACE(REPLACE(sp.sampling_point_description, CHR(13), ''), CHR(10), ''),
        '\s*\[[[:digit:]]+\]\s*$',''
    ))                          AS "Sampling point description",
    sp.line                     AS "LINE-1",
    usr.NAME                    AS "Owner",
    sp.product_code             AS "Product Code",
    sp.product_description      AS "Product Description",
    sp.cig_product_code         AS "CIG_PRODUCT_CODE",
    sp.cig_product_description  AS "CIG_PRODUCT_DESCRIPTION",
    sp.spec_group               AS "Spec_Group",
    proj.NAME                   AS "Task Plan Project",
    runset.RUNSET_ID            AS "Task Plan ID",
    rt.LIFE_CYCLE_STATE         AS "Task Status",
    p.DISPLAY_NAME              AS "Characteristic",
    pv.INTERPRETATION           AS "Compose Details",
    pv.VALUE_STRING             AS "Result",
    pv.VALUE_TEXT               AS "Formatted result",
    pv.LAST_UPDATED             AS "Result entered",
    cs.NAME                     AS "Collaboration Space",
    'MANUAL'                    AS "Result Source",
    uom.description             AS "UOM",
    (SELECT MAX(pee2.SOURCE_POSITION||':'||pee2.ITEM_STATES)
     FROM hub_owner.PEX_PROC_EXEC pe2
     JOIN hub_owner.PEX_PROC_ELEM_EXEC pee2 ON pee2.PARENT_ID = pe2.ID
     WHERE rt.WORK_ITEM LIKE '%' || LOWER(
                SUBSTR(RAWTOHEX(pe2.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe2.ID),9,4)||'-'||
                SUBSTR(RAWTOHEX(pe2.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe2.ID),17,4)||'-'||
                SUBSTR(RAWTOHEX(pe2.ID),21,12)) || '%'
       AND pee2.ITEM_STATES IS NOT NULL
       AND REGEXP_LIKE(pee2.ITEM_STATES, '[XD]')
    )                           AS "Item States (Debug)",
    rp.tp_project_plan          AS "Task Plan Project Plan"
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
     ON runset.PROJECT_ID = proj.ID
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
-- Column order matches manual exactly
-- =========================
SELECT
    s.NAME,                                                                          -- 1  Sample Name
    s.SAMPLE_ID,                                                                     -- 2  Sample ID
    -- Sample Status: identical scalar subquery to manual section.
    -- Correlates via rt.WORK_ITEM (same as manual) so always finds the right PEX.
    (SELECT CASE                                                                     -- 3  Sample Status
                WHEN MAX(CASE WHEN
                         UPPER(SUBSTR(pee2.ITEM_STATES,
                             (SELECT COUNT(*) + 1
                              FROM hub_owner.REQ_TASK rt2
                              JOIN hub_owner.SAM_SAMPLE s2
                                   ON INSTR(','||rt2.SAMPLE_LIST||',', ','||s2.SAMPLE_ID||',') > 0
                              WHERE rt2.WORK_ITEM LIKE '%' || LOWER(
                                          SUBSTR(RAWTOHEX(pe2.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe2.ID),9,4)||'-'||
                                          SUBSTR(RAWTOHEX(pe2.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe2.ID),17,4)||'-'||
                                          SUBSTR(RAWTOHEX(pe2.ID),21,12)) || '%'
                                AND s2.SAMPLE_ID < s.SAMPLE_ID),
                             1)) = 'X' THEN 1 END) = 1 THEN 'abandoned'
                WHEN MAX(CASE WHEN
                         UPPER(SUBSTR(pee2.ITEM_STATES,
                             (SELECT COUNT(*) + 1
                              FROM hub_owner.REQ_TASK rt2
                              JOIN hub_owner.SAM_SAMPLE s2
                                   ON INSTR(','||rt2.SAMPLE_LIST||',', ','||s2.SAMPLE_ID||',') > 0
                              WHERE rt2.WORK_ITEM LIKE '%' || LOWER(
                                          SUBSTR(RAWTOHEX(pe2.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe2.ID),9,4)||'-'||
                                          SUBSTR(RAWTOHEX(pe2.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe2.ID),17,4)||'-'||
                                          SUBSTR(RAWTOHEX(pe2.ID),21,12)) || '%'
                                AND s2.SAMPLE_ID < s.SAMPLE_ID),
                             1)) = 'D' THEN 1 END) = 1 THEN 'completed'
                ELSE s.LIFE_CYCLE_STATE
            END
     FROM hub_owner.PEX_PROC_EXEC pe2
     JOIN hub_owner.PEX_PROC_ELEM_EXEC pee2 ON pee2.PARENT_ID = pe2.ID
     WHERE rt.WORK_ITEM LIKE '%' || LOWER(
                SUBSTR(RAWTOHEX(pe2.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe2.ID),9,4)||'-'||
                SUBSTR(RAWTOHEX(pe2.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe2.ID),17,4)||'-'||
                SUBSTR(RAWTOHEX(pe2.ID),21,12)) || '%'
       AND pee2.ITEM_STATES IS NOT NULL
    ),
    ms.SAMPLE_ID,                                                                    -- 4  Master Sample ID
    sp.sampling_point,                                                               -- 5  Sampling point
    TRIM(REGEXP_REPLACE(                                                             -- 6  Sampling point description
        REPLACE(REPLACE(sp.sampling_point_description, CHR(13), ''), CHR(10), ''),
        '\s*\[[[:digit:]]+\]\s*$',''
    )),
    sp.line,                                                                         -- 7  LINE-1
    usr.NAME,                                                                        -- 8  Owner
    sp.product_code,                                                                 -- 9  Product Code
    sp.product_description,                                                          -- 10 Product Description
    sp.cig_product_code,                                                             -- 11 CIG_PRODUCT_CODE
    sp.cig_product_description,                                                      -- 12 CIG_PRODUCT_DESCRIPTION
    sp.spec_group,                                                                   -- 13 Spec_Group
    proj.NAME,                                                                       -- 14 Task Plan Project
    runset.RUNSET_ID,                                                                -- 15 Task Plan ID
    rt.LIFE_CYCLE_STATE,                                                             -- 16 Task Status
    MAX(CASE WHEN pv.VALUE_STRING IS NOT NULL THEN pv.VALUE_STRING END),             -- 17 Characteristic
    NULL,                                                                            -- 18 Compose Details
    TO_CHAR(MAX(CASE WHEN pv.VALUE_NUMERIC IS NOT NULL THEN pv.VALUE_NUMERIC END)),  -- 19 Result
    TO_CHAR(MAX(CASE WHEN pv.VALUE_NUMERIC IS NOT NULL THEN pv.VALUE_NUMERIC END)),  -- 20 Formatted result
    MAX(pv.LAST_UPDATED),                                                            -- 21 Result entered
    cs.NAME,                                                                         -- 22 Collaboration Space
    'EQUIPMENT',                                                                     -- 23 Result Source
    MAX(uom.description),                                                            -- 24 UOM
    (SELECT MAX(pee2.SOURCE_POSITION||':'||pee2.ITEM_STATES)                        -- 25 Item States (Debug)
     FROM hub_owner.PEX_PROC_ELEM_EXEC pee2
     WHERE pee2.PARENT_ID = pe.ID
       AND pee2.ITEM_STATES IS NOT NULL
       AND REGEXP_LIKE(pee2.ITEM_STATES, '[XD]')
    ),
    rp.tp_project_plan                                                               -- 26 Task Plan Project Plan
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
LEFT JOIN sample_properties sp
     ON sp.sample_raw_id = s.ID
LEFT JOIN hub_owner.REQ_RUNSET runset
     ON rt.RUNSET_ID = runset.ID
LEFT JOIN hub_owner.RES_PROJECT proj
     ON runset.PROJECT_ID = proj.ID
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
    s.NAME, s.SAMPLE_ID, s.LIFE_CYCLE_STATE,
    ms.SAMPLE_ID, sp.sampling_point,
    sp.sampling_point_description, sp.line,
    usr.NAME, sp.product_code, sp.product_description,
    sp.cig_product_code, sp.cig_product_description,
    sp.spec_group, proj.NAME, runset.RUNSET_ID,
    rt.LIFE_CYCLE_STATE, rt.WORK_ITEM, rt.SAMPLE_LIST, cs.NAME, peep.ID,
    pe.ID, meas_s.ROW_INDEX, rp.tp_project_plan
HAVING MAX(CASE WHEN pv.VALUE_NUMERIC IS NOT NULL
                THEN pv.VALUE_NUMERIC END) IS NOT NULL;