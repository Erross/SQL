WITH fallback_measurements AS (
  SELECT
    meas_s.context_id,
    meas_s.mapped_sample_id,
    meas_s.row_index,
    ROW_NUMBER() OVER (
      PARTITION BY meas_s.context_id
      ORDER BY meas_s.row_index, meas_s.id
    ) - 1 AS derived_item_index
  FROM hub_owner.res_measurementsample meas_s
),

target_sample AS (
  SELECT s.id AS sample_raw_id
  FROM hub_owner.sam_sample s
  WHERE s.sample_id = 'S005460'
),

target_context AS (
  SELECT
    fm.context_id,
    fm.derived_item_index
  FROM fallback_measurements fm
  JOIN target_sample ts
    ON fm.mapped_sample_id = ts.sample_raw_id
),

equipment_named AS (
  SELECT
    pe.id AS proc_exec_id,
    pee.id AS proc_elem_exec_id,
    peep.id AS peep_id,
    peep.source_position,
    pv.item_index,
    NVL(pv.group_index,1) AS group_index,
    LOWER(TRIM(MAX(CASE
      WHEN pv.value_key = 'AE'
       AND pv.value_type = 'Equipment'
      THEN pv.value_string
    END))) AS field_name,
    MAX(CASE WHEN pv.value_key = 'A' THEN pv.value_string END) AS value_string,
    MAX(CASE WHEN pv.value_key = 'A' THEN pv.value_numeric END) AS value_numeric,
    MAX(CASE WHEN pv.value_key = 'A' THEN pv.value_text END) AS value_text,
    MAX(CASE WHEN pv.value_key = 'A' THEN pv.value_numeric_text END) AS value_numeric_text
  FROM hub_owner.pex_proc_exec pe
  JOIN hub_owner.pex_proc_elem_exec pee
    ON pee.parent_id = pe.id
  JOIN hub_owner.pex_proc_elem_exec_param peep
    ON peep.parent_id = pee.id
  JOIN hub_owner.cor_parameter_value pv
    ON pv.parent_identity = peep.id
  GROUP BY pe.id, pee.id, peep.id, peep.source_position, pv.item_index, NVL(pv.group_index,1)
),

equipment_with_context AS (
  SELECT
    en.*,
    ctx.id AS context_id
  FROM equipment_named en
  JOIN hub_owner.pex_proc_elem_exec pee
    ON pee.id = en.proc_elem_exec_id
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
)

SELECT
  ewc.proc_exec_id,
  ewc.proc_elem_exec_id,
  ewc.item_index,
  ewc.group_index,
  ewc.field_name,
  ewc.value_numeric,
  ewc.value_numeric_text,
  ewc.value_text,
  ewc.value_string
FROM equipment_with_context ewc
JOIN target_context tc
  ON tc.context_id = ewc.context_id
 AND tc.derived_item_index = ewc.item_index
WHERE
  (
    ewc.value_numeric IS NOT NULL OR
    ewc.value_numeric_text IS NOT NULL OR
    ewc.value_text IS NOT NULL OR
    ewc.value_string IS NOT NULL
  )
ORDER BY
  ewc.proc_elem_exec_id,
  ewc.item_index,
  ewc.source_position,
  ewc.field_name;