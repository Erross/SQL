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

/*
Grain:
(sample, runset, task, parameter_value_row)

Results path (working in your tenant):
REQ_TASK -> REQ_TASK_PARAMETER -> COR_PARAMETER -> COR_PARAMETER_VALUE
Sample linkage:
REGEXP_SUBSTR(REQ_TASK.SAMPLE_LIST, '[^,]+', 1, ITEM_INDEX+1) = SAM_SAMPLE.SAMPLE_ID
*/

WITH sample_props AS (
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
      'Spec group'
    )
  GROUP BY oi.object_id
),

task_param_results AS (
  SELECT
    -- sample id as stored in task.sample_list (comma-separated), item_index is 0-based
    REGEXP_SUBSTR(rt.sample_list, '[^,]+', 1, pv.item_index + 1) AS matched_sample_id,

    rt.id        AS req_task_row_id,
    rt.task_id   AS task_id_text,
    rt.task_name,
    rt.method_id,

    pv.item_index,
    pv.value_numeric,
    pv.value_text AS formatted_result,

    p.name AS parameter_name

  FROM hub_owner.cor_parameter_value pv
  JOIN hub_owner.cor_parameter p
    ON pv.parent_identity = p.id
  JOIN hub_owner.req_task_parameter rtp
    ON rtp.parameter_id = p.id
  JOIN hub_owner.req_task rt
    ON rt.id = rtp.task_id
)

SELECT
  s.name      AS sample_name,
  s.sample_id AS sample_id,

  sp.sampling_point,
  sp.sampling_point_description,
  sp.line,

  u.name AS owner,

  sp.product_code,
  sp.product_description,
  sp.cig_product_code,
  sp.cig_product_description,
  sp.spec_group,

  rp.name            AS task_plan_project,
  t.life_cycle_state AS task_status,
  t.task_id          AS task_id,
  t.task_name        AS task_name,

  -- appended results
  r.parameter_name,
  r.value_numeric,
  r.formatted_result,
  r.item_index,
  r.method_id

FROM hub_owner.sam_sample s
JOIN hub_owner.req_runset_sample rss
  ON rss.sample_id = s.id
JOIN hub_owner.req_runset rs
  ON rs.id = rss.runset_id
JOIN hub_owner.req_task t
  ON t.runset_id = rs.id

LEFT JOIN hub_owner.res_project rp
  ON rp.id = rs.project_id

LEFT JOIN hub_owner.sec_user u
  ON u.id = s.owner_id

LEFT JOIN sample_props sp
  ON sp.sample_raw_id = s.id

-- ✅ the real results join:
LEFT JOIN task_param_results r
  ON r.matched_sample_id = s.sample_id
 AND r.req_task_row_id   = t.id

ORDER BY
  s.name, s.sample_id, t.task_id, r.parameter_name, r.item_index;
