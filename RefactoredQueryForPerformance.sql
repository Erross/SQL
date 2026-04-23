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

sample_properties AS (
  SELECT
    oi.object_id AS sample_raw_id,
    MAX(CASE WHEN p.display_label = 'Sampling Point'
             THEN COALESCE(pv.string_value,
                           SUBSTR(TO_CHAR(pv.long_string_value), 1, 4000),
                           TO_CHAR(pv.number_value))
        END) AS sampling_point,
    MAX(CASE WHEN p.display_label = 'Sampling Point Description'
             THEN COALESCE(pv.string_value,
                           SUBSTR(TO_CHAR(pv.long_string_value), 1, 4000))
        END) AS sampling_point_description,
    MAX(CASE WHEN p.display_label = 'Product Code'
             THEN COALESCE(pv.string_value,
                           SUBSTR(TO_CHAR(pv.long_string_value), 1, 4000),
                           TO_CHAR(pv.number_value))
        END) AS product_code,
    MAX(CASE WHEN p.display_label = 'Product Description'
             THEN COALESCE(pv.string_value,
                           SUBSTR(TO_CHAR(pv.long_string_value), 1, 4000))
        END) AS product_description
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
      'Product Code',
      'Product Description'
    )
  GROUP BY oi.object_id
),

/* ============================================================
   EQUIPMENT PARAMETER METADATA / VALUES
   ============================================================ */
equipment_param_defs AS (
  SELECT
    pe.id  AS proc_exec_id,
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
    pe.id  AS proc_exec_id,
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
    d.field_name,
    v.value_string,
    v.value_numeric,
    v.value_text,
    v.value_numeric_text,
    v.interpretation,
    v.last_updated,
    v.unit_id
  FROM equipment_param_values v
  JOIN equipment_param_defs d
    ON d.peep_id = v.peep_id
),

equipment_classified AS (
  SELECT
    en.*,
    CASE
      WHEN en.field_name IS NULL THEN 'LABEL'
      WHEN en.field_name IN (
        'sample_id',
        'data_id',
        'meter_number',
        'meter number',
        'sampling_point_time',
        'sampling point time',
        'sampling point time *'
      ) THEN 'METADATA'
      ELSE 'CANDIDATE_RESULT'
    END AS field_role
  FROM equipment_named en
),

equipment_packet_shape AS (
  SELECT
    proc_elem_exec_id,
    MAX(item_index) AS max_item_index
  FROM equipment_classified
  GROUP BY proc_elem_exec_id
),

equipment_result_candidates AS (
  SELECT
    ec.proc_exec_id,
    ec.proc_elem_exec_id,
    ec.item_index,
    ec.group_index,
    ec.field_name,
    ec.value_numeric,
    ec.value_numeric_text,
    ec.value_text,
    ec.value_string,
    ec.interpretation,
    ec.last_updated,
    ec.unit_id,
    ROW_NUMBER() OVER (
      PARTITION BY ec.proc_exec_id, ec.proc_elem_exec_id, ec.item_index, ec.group_index
      ORDER BY
        CASE
          WHEN ec.value_numeric IS NOT NULL THEN 1
          WHEN ec.value_numeric_text IS NOT NULL THEN 2
          WHEN ec.value_text IS NOT NULL THEN 3
          WHEN ec.value_string IS NOT NULL THEN 4
          ELSE 9
        END,
        ec.source_position,
        ec.field_name
    ) AS rn
  FROM equipment_classified ec
  WHERE ec.field_role = 'CANDIDATE_RESULT'
    AND (
      ec.value_numeric IS NOT NULL OR
      ec.value_numeric_text IS NOT NULL OR
      ec.value_text IS NOT NULL OR
      ec.value_string IS NOT NULL
    )
),

equipment_selected_result AS (
  SELECT
    proc_exec_id,
    proc_elem_exec_id,
    item_index,
    group_index,
    field_name AS result_field_name,
    value_numeric AS result_numeric,
    COALESCE(value_numeric_text, value_text, value_string, TO_CHAR(value_numeric)) AS result_text,
    interpretation AS result_interpretation,
    last_updated AS result_entered,
    unit_id AS result_unit_id
  FROM equipment_result_candidates
  WHERE rn = 1
),

equipment_row_metadata AS (
  SELECT
    proc_exec_id,
    proc_elem_exec_id,
    item_index,
    group_index,
    MAX(CASE WHEN field_name = 'sample_id'
             THEN COALESCE(value_string, value_text, value_numeric_text, TO_CHAR(value_numeric))
        END) AS packet_sample_id
  FROM equipment_classified
  GROUP BY proc_exec_id, proc_elem_exec_id, item_index, group_index
),

equipment_rows AS (
  SELECT
    m.proc_exec_id,
    m.proc_elem_exec_id,
    m.item_index,
    m.group_index,
    m.packet_sample_id,
    r.result_field_name,
    r.result_numeric,
    r.result_text,
    r.result_interpretation,
    r.result_entered,
    r.result_unit_id
  FROM equipment_row_metadata m
  LEFT JOIN equipment_selected_result r
    ON r.proc_exec_id = m.proc_exec_id
   AND r.proc_elem_exec_id = m.proc_elem_exec_id
   AND r.item_index = m.item_index
   AND r.group_index = m.group_index
),

equipment_with_context AS (
  SELECT
    er.*,
    ctx.id AS context_id
  FROM equipment_rows er
  JOIN hub_owner.pex_proc_elem_exec pee
    ON pee.id = er.proc_elem_exec_id
  LEFT JOIN hub_owner.res_retrieval_context ctx
    ON ctx.context =
       'urn:pexelement:' ||
       LOWER(
         SUBSTR(RAWTOHEX(pee.id),1,8)||'-'||
         SUBSTR(RAWTOHEX(pee.id),9,4)||'-'||
         SUBSTR(RAWTOHEX(pee.id),13,4)||'-'||
         SUBSTR(RAWTOHEX(pee.id),17,4)||'-'||
         SUBSTR(RAWTOHEX(pee.id),21,12)
       )
),

fallback_measurements AS (
  SELECT
    meas_s.context_id,
    meas_s.mapped_sample_id,
    ROW_NUMBER() OVER (
      PARTITION BY meas_s.context_id
      ORDER BY meas_s.row_index, meas_s.id
    ) - 1 AS derived_item_index
  FROM hub_owner.res_measurementsample meas_s
),

equipment_resolved AS (
  SELECT
    ewc.proc_exec_id,
    ewc.proc_elem_exec_id,
    ewc.item_index,
    ewc.group_index,
    ewc.result_field_name,
    ewc.result_numeric,
    ewc.result_text,
    ewc.result_interpretation,
    ewc.result_entered,
    ewc.result_unit_id,

    COALESCE(
      s_packet.id,
      s_fb_multi.id,
      s_fb_single.id,
      s_fb_task.id
    ) AS resolved_sample_raw_id
  FROM equipment_with_context ewc
  LEFT JOIN equipment_packet_shape eps
    ON eps.proc_elem_exec_id = ewc.proc_elem_exec_id

  LEFT JOIN hub_owner.sam_sample s_packet
    ON s_packet.sample_id = ewc.packet_sample_id

  LEFT JOIN fallback_measurements fm_multi
    ON fm_multi.context_id = ewc.context_id
   AND fm_multi.derived_item_index = ewc.item_index
   AND ewc.packet_sample_id IS NULL
   AND NVL(eps.max_item_index, 0) > 0

  LEFT JOIN hub_owner.sam_sample s_fb_multi
    ON s_fb_multi.id = fm_multi.mapped_sample_id

  LEFT JOIN fallback_measurements fm_single
    ON fm_single.context_id = ewc.context_id
   AND ewc.packet_sample_id IS NULL
   AND NVL(eps.max_item_index, 0) = 0

  LEFT JOIN hub_owner.sam_sample s_fb_single
    ON s_fb_single.id = fm_single.mapped_sample_id

  LEFT JOIN task_map tm_resolve
    ON tm_resolve.proc_exec_id = ewc.proc_exec_id

  LEFT JOIN hub_owner.sam_sample s_fb_task
    ON s_fb_task.sample_id =
         REGEXP_SUBSTR(tm_resolve.sample_list, '[^,]+', 1, ewc.item_index + 1)
   AND s_packet.id IS NULL
),

manual_results AS (
  SELECT DISTINCT
    s.name AS "Sample Name",
    s.sample_id AS "Sample ID",
    s.life_cycle_state AS "Sample Status",
    ms.sample_id AS "Master Sample ID",
    sp.sampling_point AS "Sampling point",
    TRIM(REGEXP_REPLACE(
      REPLACE(REPLACE(sp.sampling_point_description, CHR(13), ''), CHR(10), ''),
      '\s*\[[[:digit:]]+\]\s*$',''
    )) AS "Sampling point description",
    sp.product_code AS "Product Code",
    sp.product_description AS "Product Description",
    runset.runset_id AS "Task Plan ID",
    tm.task_status AS "Task Status",
    p.display_name AS "Characteristic",
    pv.value_string AS "Result",
    pv.value_text AS "Formatted result",
    pv.last_updated AS "Result entered",
    'MANUAL' AS "Result Source",
    uom.description AS "UOM"
  FROM hub_owner.cor_parameter_value pv
  JOIN hub_owner.cor_parameter p
    ON pv.parent_identity = p.id
  JOIN hub_owner.req_task_parameter rtp
    ON p.id = rtp.parameter_id
  JOIN hub_owner.req_task rt
    ON rtp.task_id = rt.id
  LEFT JOIN task_map tm
    ON tm.task_raw_id = rt.id
  LEFT JOIN hub_owner.req_runset runset
    ON rt.runset_id = runset.id
  LEFT JOIN hub_owner.sam_sample s
    ON s.sample_id = REGEXP_SUBSTR(rt.sample_list, '[^,]+', 1, pv.item_index + 1)
  LEFT JOIN hub_owner.sam_sample ms
    ON s.master_sample_id = ms.id
  LEFT JOIN sample_properties sp
    ON sp.sample_raw_id = s.id
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
    s.life_cycle_state AS "Sample Status",
    ms.sample_id AS "Master Sample ID",
    sp.sampling_point AS "Sampling point",
    TRIM(REGEXP_REPLACE(
      REPLACE(REPLACE(sp.sampling_point_description, CHR(13), ''), CHR(10), ''),
      '\s*\[[[:digit:]]+\]\s*$',''
    )) AS "Sampling point description",
    sp.product_code AS "Product Code",
    sp.product_description AS "Product Description",
    runset.runset_id AS "Task Plan ID",
    tm.task_status AS "Task Status",
    NVL(er.result_field_name, 'Equipment Result') AS "Characteristic",
    TO_CHAR(er.result_numeric) AS "Result",
    er.result_text AS "Formatted result",
    er.result_entered AS "Result entered",
    'EQUIPMENT' AS "Result Source",
    uom.description AS "UOM"
  FROM equipment_resolved er
  JOIN task_map tm
    ON tm.proc_exec_id = er.proc_exec_id
  JOIN hub_owner.sam_sample s
    ON s.id = er.resolved_sample_raw_id
  LEFT JOIN hub_owner.sam_sample ms
    ON s.master_sample_id = ms.id
  LEFT JOIN sample_properties sp
    ON sp.sample_raw_id = s.id
  LEFT JOIN hub_owner.req_runset runset
    ON tm.runset_id = runset.id
  LEFT JOIN hub_owner.cospc_object_identity coi_sample
    ON coi_sample.object_id = s.id
  LEFT JOIN hub_owner.sec_collab_space cs
    ON cs.id = coi_sample.collaborative_space_id
  LEFT JOIN hub_owner.cor_unit uom
    ON uom.id = er.result_unit_id
  WHERE er.resolved_sample_raw_id IS NOT NULL
    AND cs.id = '5FD74EE88C024C2EB908BCE0E176B0E8'
    AND ms.sample_id != 'planned'
    AND (
         er.result_numeric IS NOT NULL OR
         er.result_text IS NOT NULL
        )
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