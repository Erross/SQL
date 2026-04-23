WITH equipment_param_defs AS (
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
    MAX(item_index) AS max_item_index,
    COUNT(DISTINCT item_index) AS item_index_count
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
        END) AS packet_sample_id,
    MAX(CASE WHEN field_name = 'data_id'
             THEN COALESCE(value_string, value_text, value_numeric_text, TO_CHAR(value_numeric))
        END) AS data_id,
    MAX(CASE WHEN field_name IN ('meter_number', 'meter number')
             THEN COALESCE(value_string, value_text, value_numeric_text, TO_CHAR(value_numeric))
        END) AS meter_number
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
    m.data_id,
    m.meter_number,
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
    pee.id AS pee_id,
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
    meas_s.row_index,
    meas_s.id AS meas_sample_id,
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
    ewc.packet_sample_id,
    ewc.data_id,
    ewc.meter_number,
    ewc.result_field_name,
    ewc.result_numeric,
    ewc.result_text,
    ewc.result_interpretation,
    ewc.result_entered,
    ewc.result_unit_id,
    ewc.context_id,
    eps.max_item_index,
    s_packet.id AS packet_sample_raw_id,
    s_packet.sample_id AS packet_sample_id_resolved,
    s_fb_multi.id AS fallback_multi_sample_raw_id,
    s_fb_multi.sample_id AS fallback_multi_sample_id_resolved,
    s_fb_single.id AS fallback_single_sample_raw_id,
    s_fb_single.sample_id AS fallback_single_sample_id_resolved,
    COALESCE(s_packet.id, s_fb_multi.id, s_fb_single.id) AS resolved_sample_raw_id,
    COALESCE(s_packet.sample_id, s_fb_multi.sample_id, s_fb_single.sample_id) AS resolved_sample_id
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
   AND EXISTS (
     SELECT 1
     FROM hub_owner.req_task rt2
     JOIN hub_owner.sam_sample s2
       ON s2.id = fm_single.mapped_sample_id
     WHERE rt2.work_item LIKE '%' || LOWER(
           SUBSTR(RAWTOHEX(ewc.proc_exec_id),1,8)||'-'||
           SUBSTR(RAWTOHEX(ewc.proc_exec_id),9,4)||'-'||
           SUBSTR(RAWTOHEX(ewc.proc_exec_id),13,4)||'-'||
           SUBSTR(RAWTOHEX(ewc.proc_exec_id),17,4)||'-'||
           SUBSTR(RAWTOHEX(ewc.proc_exec_id),21,12)
         ) || '%'
       AND INSTR(','||rt2.sample_list||',', ','||s2.sample_id||',') > 0
   )
  LEFT JOIN hub_owner.sam_sample s_fb_single
    ON s_fb_single.id = fm_single.mapped_sample_id
)
SELECT
  er.proc_exec_id,
  er.proc_elem_exec_id,
  er.item_index,
  er.group_index,
  er.context_id,
  er.max_item_index,
  er.packet_sample_id,
  er.packet_sample_id_resolved,
  er.fallback_multi_sample_id_resolved,
  er.fallback_single_sample_id_resolved,
  er.resolved_sample_id,
  er.result_field_name,
  er.result_numeric,
  er.result_text,
  er.result_interpretation,
  er.result_entered,
  rt.task_id,
  rt.life_cycle_state AS task_status,
  s.sample_id AS final_join_sample_id,
  CASE WHEN er.resolved_sample_raw_id IS NOT NULL THEN 'Y' ELSE 'N' END AS has_resolved_sample,
  CASE WHEN rt.id IS NOT NULL THEN 'Y' ELSE 'N' END AS has_task_match,
  CASE WHEN s.id IS NOT NULL THEN 'Y' ELSE 'N' END AS has_final_sample_join
FROM equipment_resolved er
LEFT JOIN hub_owner.pex_proc_exec pe
  ON pe.id = er.proc_exec_id
LEFT JOIN hub_owner.req_task rt
  ON rt.work_item LIKE '%' || LOWER(
       SUBSTR(RAWTOHEX(pe.id),1,8)||'-'||
       SUBSTR(RAWTOHEX(pe.id),9,4)||'-'||
       SUBSTR(RAWTOHEX(pe.id),13,4)||'-'||
       SUBSTR(RAWTOHEX(pe.id),17,4)||'-'||
       SUBSTR(RAWTOHEX(pe.id),21,12)
     ) || '%'
LEFT JOIN hub_owner.sam_sample s
  ON s.id = er.resolved_sample_raw_id
WHERE
  (
    er.packet_sample_id = 'S005460'
    OR er.packet_sample_id_resolved = 'S005460'
    OR er.fallback_multi_sample_id_resolved = 'S005460'
    OR er.fallback_single_sample_id_resolved = 'S005460'
    OR er.resolved_sample_id = 'S005460'
  )
   OR (
    er.result_field_name = 'ov_meter_reading'
    AND NVL(er.result_text, TO_CHAR(er.result_numeric)) = '14.87'
  )
ORDER BY er.proc_exec_id, er.proc_elem_exec_id, er.item_index, er.group_index;