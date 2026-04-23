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
        MAX(CASE WHEN pv.value_key = 'A' THEN pv.last_updated END)       AS last_updated
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
        v.last_updated
    FROM equipment_param_values v
    JOIN equipment_param_defs d
      ON d.peep_id = v.peep_id
)
SELECT
    n.proc_exec_id,
    n.proc_elem_exec_id,
    n.group_index,
    n.item_index,
    n.source_position,
    n.field_name,
    n.value_string,
    n.value_numeric,
    n.value_text,
    n.value_numeric_text,
    n.interpretation,
    n.last_updated
FROM equipment_named n
WHERE EXISTS (
    SELECT 1
    FROM equipment_named s
    WHERE s.proc_exec_id = n.proc_exec_id
      AND s.proc_elem_exec_id = n.proc_elem_exec_id
      AND s.group_index = n.group_index
      AND s.item_index = n.item_index
      AND s.field_name = 'sample_id'
      AND COALESCE(s.value_string, s.value_text, s.value_numeric_text, TO_CHAR(s.value_numeric)) = 'S005460'
)
ORDER BY
    n.proc_exec_id,
    n.proc_elem_exec_id,
    n.group_index,
    n.item_index,
    n.source_position,
    n.field_name;

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
equipment_rows AS (
    SELECT
        proc_exec_id,
        proc_elem_exec_id,
        item_index,
        group_index,
        MAX(CASE WHEN field_name = 'sample_id'
                 THEN COALESCE(value_string, value_text, value_numeric_text, TO_CHAR(value_numeric))
            END) AS packet_sample_id,
        MAX(CASE WHEN field_name IN (
                        'ov_meter_reading',
                        'ov meter reading [%]',
                        'ov meter reading [%] *',
                        'ov_meter_reading_[%]'
                 )
                 THEN value_numeric
            END) AS result_numeric,
        MAX(CASE WHEN field_name IN (
                        'ov_meter_reading',
                        'ov meter reading [%]',
                        'ov meter reading [%] *',
                        'ov_meter_reading_[%]'
                 )
                 THEN COALESCE(value_string, value_text, value_numeric_text, TO_CHAR(value_numeric))
            END) AS result_text
    FROM equipment_named
    GROUP BY proc_exec_id, proc_elem_exec_id, item_index, group_index
)
SELECT
    er.packet_sample_id,
    er.item_index,
    er.group_index,
    er.result_numeric,
    er.result_text,
    s.sample_id AS matched_sample_id,
    rt.task_id,
    rt.life_cycle_state AS task_status,
    rt.sample_list,
    ms.sample_id AS master_sample_id,
    cs.id AS cs_id,
    cs.name AS cs_name,
    CASE WHEN s.id IS NOT NULL THEN 'Y' ELSE 'N' END AS matched_sample,
    CASE WHEN rt.id IS NOT NULL THEN 'Y' ELSE 'N' END AS matched_task,
    CASE WHEN INSTR(','||rt.sample_list||',', ','||s.sample_id||',') > 0 THEN 'Y' ELSE 'N' END AS in_task_sample_list,
    CASE WHEN er.result_numeric IS NOT NULL THEN 'Y' ELSE 'N' END AS has_numeric_result
FROM equipment_rows er
LEFT JOIN hub_owner.pex_proc_exec pe
  ON pe.id = er.proc_exec_id
LEFT JOIN hub_owner.req_task rt
  ON rt.work_item LIKE '%' || LOWER(
         SUBSTR(RAWTOHEX(pe.id),1,8)||'-'||SUBSTR(RAWTOHEX(pe.id),9,4)||'-'||
         SUBSTR(RAWTOHEX(pe.id),13,4)||'-'||SUBSTR(RAWTOHEX(pe.id),17,4)||'-'||
         SUBSTR(RAWTOHEX(pe.id),21,12)
     ) || '%'
LEFT JOIN hub_owner.sam_sample s
  ON s.sample_id = er.packet_sample_id
LEFT JOIN hub_owner.sam_sample ms
  ON s.master_sample_id = ms.id
LEFT JOIN hub_owner.cospc_object_identity coi
  ON coi.object_id = s.id
LEFT JOIN hub_owner.sec_collab_space cs
  ON cs.id = coi.collaborative_space_id
WHERE er.packet_sample_id = 'S005460'
ORDER BY er.proc_exec_id, er.proc_elem_exec_id, er.group_index, er.item_index;  

SELECT
    pe.id  AS proc_exec_id,
    pee.id AS proc_elem_exec_id,
    peep.id AS peep_id,
    peep.source_position,
    pv.item_index,
    NVL(pv.group_index, 1) AS group_index,
    pv.value_key,
    pv.value_type,
    pv.value_string,
    pv.value_numeric,
    pv.value_text,
    pv.value_numeric_text,
    pv.interpretation,
    pv.last_updated
FROM hub_owner.pex_proc_exec pe
JOIN hub_owner.pex_proc_elem_exec pee
  ON pee.parent_id = pe.id
JOIN hub_owner.pex_proc_elem_exec_param peep
  ON peep.parent_id = pee.id
JOIN hub_owner.cor_parameter_value pv
  ON pv.parent_identity = peep.id
WHERE UPPER(NVL(pv.value_string, '')) LIKE '%S005460%'
   OR UPPER(NVL(pv.value_text, '')) LIKE '%S005460%'
   OR UPPER(NVL(pv.value_numeric_text, '')) LIKE '%S005460%'
ORDER BY pv.last_updated DESC, peep.source_position, pv.item_index;