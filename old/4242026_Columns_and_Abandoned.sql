WITH task_map AS (
  SELECT
    pe.id AS proc_exec_id,
    rt.id AS task_raw_id,
    rt.task_id,
    rt.runset_id,
    rt.sample_list,
    rt.life_cycle_state AS task_status
  FROM hub_owner.pex_proc_exec pe
  JOIN hub_owner.req_task rt
    ON rt.work_item LIKE '%' || LOWER(
         SUBSTR(RAWTOHEX(pe.id),1,8)||'-'||
         SUBSTR(RAWTOHEX(pe.id),9,4)||'-'||
         SUBSTR(RAWTOHEX(pe.id),13,4)||'-'||
         SUBSTR(RAWTOHEX(pe.id),17,4)||'-'||
         SUBSTR(RAWTOHEX(pe.id),21,12)
       ) || '%'
  WHERE rt.life_cycle_state IN ('released', 'completed')
),

sample_positions AS (
  SELECT
    tm.proc_exec_id,
    tm.task_raw_id,
    tm.task_id,
    tm.task_status,
    REGEXP_SUBSTR(tm.sample_list, '[^,]+', 1, LEVEL) AS sample_id,
    LEVEL - 1 AS sample_pos
  FROM task_map tm
  CONNECT BY REGEXP_SUBSTR(tm.sample_list, '[^,]+', 1, LEVEL) IS NOT NULL
         AND PRIOR tm.task_raw_id = tm.task_raw_id
         AND PRIOR SYS_GUID() IS NOT NULL
),

item_state_flags AS (
  SELECT
    pe.id AS proc_exec_id,
    pee.item_states
  FROM hub_owner.pex_proc_exec pe
  JOIN hub_owner.pex_proc_elem_exec pee
    ON pee.parent_id = pe.id
  WHERE pee.item_states IS NOT NULL
),

sample_exec_status AS (
  SELECT
    sp.proc_exec_id,
    sp.sample_id,
    CASE
      WHEN MAX(CASE WHEN SUBSTR(isf.item_states, sp.sample_pos + 1, 1) = 'X' THEN 1 ELSE 0 END) = 1 THEN 'abandoned'
      WHEN MAX(CASE WHEN SUBSTR(isf.item_states, sp.sample_pos + 1, 1) = 'D' THEN 1 ELSE 0 END) = 1 THEN 'completed'
      ELSE MAX(sp.task_status)
    END AS derived_status
  FROM sample_positions sp
  LEFT JOIN item_state_flags isf
    ON isf.proc_exec_id = sp.proc_exec_id
  GROUP BY sp.proc_exec_id, sp.sample_id
),

sample_properties AS (
  SELECT
    oi.object_id AS sample_raw_id,

    MAX(CASE WHEN p.display_label = 'Sampling Point'
             THEN COALESCE(
                    pv.string_value,
                    SUBSTR(TO_CHAR(pv.long_string_value), 1, 4000),
                    TO_CHAR(pv.number_value)
                  )
        END) AS sampling_point,

    MAX(CASE WHEN p.display_label = 'Sampling Point Description'
             THEN COALESCE(
                    pv.string_value,
                    SUBSTR(TO_CHAR(pv.long_string_value), 1, 4000)
                  )
        END) AS sampling_point_description,

    MAX(CASE WHEN p.display_label = 'Line'
             THEN COALESCE(
                    pv.string_value,
                    SUBSTR(TO_CHAR(pv.long_string_value), 1, 4000),
                    TO_CHAR(pv.number_value)
                  )
        END) AS line,

    MAX(CASE WHEN p.display_label = 'Product Code'
             THEN COALESCE(
                    pv.string_value,
                    SUBSTR(TO_CHAR(pv.long_string_value), 1, 4000),
                    TO_CHAR(pv.number_value)
                  )
        END) AS product_code,

    MAX(CASE WHEN p.display_label = 'Product Description'
             THEN COALESCE(
                    pv.string_value,
                    SUBSTR(TO_CHAR(pv.long_string_value), 1, 4000)
                  )
        END) AS product_description,

    MAX(CASE WHEN p.display_label = 'Cig Product Code'
             THEN COALESCE(
                    pv.string_value,
                    SUBSTR(TO_CHAR(pv.long_string_value), 1, 4000),
                    TO_CHAR(pv.number_value)
                  )
        END) AS cig_product_code,

    MAX(CASE WHEN p.display_label = 'Cig Product Description'
             THEN COALESCE(
                    pv.string_value,
                    SUBSTR(TO_CHAR(pv.long_string_value), 1, 4000)
                  )
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
    ve.name AS tp_project_plan
  FROM hub_owner.cor_class_identity ci
  JOIN hub_owner.cor_object_identity oi
    ON oi.class_identity_id = ci.id
  JOIN hub_owner.cor_property_value pv
    ON pv.object_identity_id = oi.id
  JOIN hub_owner.cor_property p
    ON p.name = pv.property_id
  JOIN hub_owner.cor_vocab_entry ve
    ON RAWTOHEX(ve.id) = UPPER(REPLACE(pv.string_value, '-', ''))
  WHERE ci.table_name = 'req_runset'
    AND p.display_label = 'Project Plan'
    AND pv.string_value IS NOT NULL
),

equipment_param_defs AS (
  SELECT
    pe.id AS proc_exec_id,
    pee.id AS proc_elem_exec_id,
    peep.id AS peep_id,
    peep.source_position,
    LOWER(TRIM(MAX(CASE
      WHEN pv.value_key = 'AE'
       AND pv.value_type = 'Equipment'
      THEN pv.value_string
    END))) AS field_name
  FROM hub_owner.pex_proc_exec pe
  JOIN hub_owner.pex_proc_elem_exec pee
    ON pee.parent_id = pe.id
  JOIN hub_owner.pex_proc_elem_exec_param peep
    ON peep.parent_id = pee.id
  JOIN hub_owner.cor_parameter_value pv
    ON pv.parent_identity = peep.id
  GROUP BY pe.id, pee.id, peep.id, peep.source_position
),

equipment_param_values AS (
  SELECT
    pe.id AS proc_exec_id,
    pee.id AS proc_elem_exec_id,
    peep.id AS peep_id,
    peep.source_position,
    pv.item_index,
    NVL(pv.group_index, 1) AS group_index,
    MAX(CASE WHEN pv.value_key = 'A' THEN pv.value_string END)       AS value_string,
    MAX(CASE WHEN pv.value_key = 'A' THEN pv.value_numeric END)      AS value_numeric,
    MAX(CASE WHEN pv.value_key = 'A' THEN pv.value_text END)         AS value_text,
    MAX(CASE WHEN pv.value_key = 'A' THEN pv.value_numeric_text END) AS value_numeric_text,
    MAX(CASE WHEN pv.value_key = 'A' THEN pv.interpretation END)     AS interpretation,
    MAX(CASE WHEN pv.value_key = 'A' THEN pv.last_updated END)       AS last_updated,
    MAX(CASE WHEN pv.value_key = 'A' THEN pv.unit END)               AS unit_id
  FROM hub_owner.pex_proc_exec pe
  JOIN hub_owner.pex_proc_elem_exec pee
    ON pee.parent_id = pe.id
  JOIN hub_owner.pex_proc_elem_exec_param peep
    ON peep.parent_id = pee.id
  JOIN hub_owner.cor_parameter_value pv
    ON pv.parent_identity = peep.id
  GROUP BY pe.id, pee.id, peep.id, peep.source_position, pv.item_index, NVL(pv.group_index, 1)
),

equipment_named AS (
  SELECT
    v.proc_exec_id,
    v.proc_elem_exec_id,
    v.peep_id,
    v.source_position,
    v.item_index,
    v.group_index,
    v.value_string,
    v.value_numeric,
    v.value_text,
    v.value_numeric_text,
    v.interpretation,
    v.last_updated,
    v.unit_id,
    d.field_name
  FROM equipment_param_values v
  JOIN equipment_param_defs d
    ON d.peep_id = v.peep_id
),

equipment_result_candidates AS (
  SELECT
    en.*,
    ROW_NUMBER() OVER (
      PARTITION BY en.proc_exec_id, en.proc_elem_exec_id, en.item_index, en.group_index
      ORDER BY en.source_position
    ) AS rn
  FROM equipment_named en
  WHERE en.value_numeric IS NOT NULL
    AND NOT (
      en.field_name LIKE '%weight%'
      OR en.field_name LIKE '%mass%'
      OR en.field_name LIKE '%tare%'
      OR en.field_name LIKE '%gross%'
      OR en.field_name LIKE '%net_weight%'
      OR en.field_name LIKE '%sample_weight%'
      OR en.field_name LIKE '%dish_weight%'
    )
),

equipment_selected_result AS (
  SELECT *
  FROM equipment_result_candidates
  WHERE rn = 1
),

equipment_resolved AS (
  SELECT
    er.*,
    s.id AS sample_raw_id,
    s.sample_id
  FROM equipment_selected_result er
  JOIN task_map tm
    ON tm.proc_exec_id = er.proc_exec_id
  JOIN hub_owner.sam_sample s
    ON s.sample_id =
         REGEXP_SUBSTR(tm.sample_list, '[^,]+', 1, er.item_index + 1)
),

manual_results AS (
  SELECT DISTINCT
    s.name AS "Sample Name",
    s.sample_id AS "Sample ID",
    ses.derived_status AS "Sample Status",
    ms.sample_id AS "Master Sample ID",
    sp.sampling_point AS "Sampling point",
    TRIM(REGEXP_REPLACE(
      REPLACE(REPLACE(sp.sampling_point_description, CHR(13), ''), CHR(10), ''),
      '\s*\[[[:digit:]]+\]\s*$',''
    )) AS "Sampling point description",
    sp.line AS "LINE-1",
    usr.name AS "Owner",
    sp.product_code AS "Product Code",
    sp.product_description AS "Product Description",
    sp.cig_product_code AS "CIG_PRODUCT_CODE",
    sp.cig_product_description AS "CIG_PRODUCT_DESCRIPTION",
    sp.spec_group AS "Spec_Group",
    proj.name AS "Task Plan Project",
    runset.runset_id AS "Task Plan ID",
    runset.date_created AS "Task Plan Creation Date",
    tm.task_status AS "Task Status",
    p.display_name AS "Characteristic",
    pv.value_string AS "Result",
    pv.value_text AS "Formatted result",
    pv.last_updated AS "Result entered",
    'MANUAL' AS "Result Source",
    uom.description AS "UOM",
    rp.tp_project_plan AS "Task Plan Project Plan"

  FROM hub_owner.cor_parameter_value pv
  JOIN hub_owner.cor_parameter p
    ON pv.parent_identity = p.id
  JOIN hub_owner.req_task_parameter rtp
    ON p.id = rtp.parameter_id
  JOIN hub_owner.req_task rt
    ON rtp.task_id = rt.id
  JOIN task_map tm
    ON tm.task_raw_id = rt.id
  JOIN sample_exec_status ses
    ON ses.sample_id = REGEXP_SUBSTR(rt.sample_list, '[^,]+', 1, pv.item_index + 1)
   AND ses.proc_exec_id = tm.proc_exec_id
  JOIN hub_owner.sam_sample s
    ON s.sample_id = ses.sample_id
  LEFT JOIN hub_owner.sam_sample ms
    ON s.master_sample_id = ms.id
  LEFT JOIN hub_owner.sec_user usr
    ON s.owner_id = usr.id
  LEFT JOIN sample_properties sp
    ON sp.sample_raw_id = s.id
  LEFT JOIN hub_owner.req_runset runset
    ON rt.runset_id = runset.id
  LEFT JOIN hub_owner.res_project proj
    ON runset.project_id = proj.id
  LEFT JOIN runset_properties rp
    ON rp.runset_raw_id = runset.id
  LEFT JOIN hub_owner.cospc_object_identity coi_sample
    ON coi_sample.object_id = s.id
  LEFT JOIN hub_owner.sec_collab_space cs
    ON cs.id = coi_sample.collaborative_space_id
  LEFT JOIN hub_owner.cor_unit uom
    ON pv.unit = uom.id
  WHERE pv.value_key = 'A'
    AND s.sample_id IS NOT NULL
    AND ms.sample_id != 'planned'
    AND p.display_name != 'Sample'
    AND p.value_type NOT IN ('Vocabulary')
    AND pv.value_string IS NOT NULL
    AND cs.id = '5FD74EE88C024C2EB908BCE0E176B0E8'
),

equipment_results AS (
  SELECT DISTINCT
    s.name AS "Sample Name",
    s.sample_id AS "Sample ID",
    ses.derived_status AS "Sample Status",
    ms.sample_id AS "Master Sample ID",
    sp.sampling_point AS "Sampling point",
    TRIM(REGEXP_REPLACE(
      REPLACE(REPLACE(sp.sampling_point_description, CHR(13), ''), CHR(10), ''),
      '\s*\[[[:digit:]]+\]\s*$',''
    )) AS "Sampling point description",
    sp.line AS "LINE-1",
    usr.name AS "Owner",
    sp.product_code AS "Product Code",
    sp.product_description AS "Product Description",
    sp.cig_product_code AS "CIG_PRODUCT_CODE",
    sp.cig_product_description AS "CIG_PRODUCT_DESCRIPTION",
    sp.spec_group AS "Spec_Group",
    proj.name AS "Task Plan Project",
    runset.runset_id AS "Task Plan ID",
    runset.date_created AS "Task Plan Creation Date",
    tm.task_status AS "Task Status",
    er.field_name AS "Characteristic",
    TO_CHAR(er.value_numeric) AS "Result",
    COALESCE(er.value_numeric_text, er.value_text, er.value_string, TO_CHAR(er.value_numeric)) AS "Formatted result",
    er.last_updated AS "Result entered",
    'EQUIPMENT' AS "Result Source",
    uom.description AS "UOM",
    rp.tp_project_plan AS "Task Plan Project Plan"

  FROM equipment_resolved er
  JOIN task_map tm
    ON tm.proc_exec_id = er.proc_exec_id
  JOIN sample_exec_status ses
    ON ses.sample_id = er.sample_id
   AND ses.proc_exec_id = tm.proc_exec_id
  JOIN hub_owner.sam_sample s
    ON s.id = er.sample_raw_id
  LEFT JOIN hub_owner.sam_sample ms
    ON s.master_sample_id = ms.id
  LEFT JOIN hub_owner.sec_user usr
    ON s.owner_id = usr.id
  LEFT JOIN sample_properties sp
    ON sp.sample_raw_id = s.id
  LEFT JOIN hub_owner.req_runset runset
    ON tm.runset_id = runset.id
  LEFT JOIN hub_owner.res_project proj
    ON runset.project_id = proj.id
  LEFT JOIN runset_properties rp
    ON rp.runset_raw_id = runset.id
  LEFT JOIN hub_owner.cor_unit uom
    ON uom.id = er.unit_id
  LEFT JOIN hub_owner.cospc_object_identity coi_sample
    ON coi_sample.object_id = s.id
  LEFT JOIN hub_owner.sec_collab_space cs
    ON cs.id = coi_sample.collaborative_space_id
  WHERE cs.id = '5FD74EE88C024C2EB908BCE0E176B0E8'
    AND ms.sample_id != 'planned'
)

SELECT *
FROM (
  SELECT * FROM manual_results
  UNION ALL
  SELECT * FROM equipment_results
)
WHERE "Sample ID" IN (
  'S007162','S007165','S007168','S007171','S007174',
  'S007178','S007181','S007184','S007187','S007190',
  'S005460'
)
ORDER BY "Sample ID", "Result Source", "Characteristic";