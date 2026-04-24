WITH task_map AS (
  SELECT
    pe.id AS proc_exec_id,
    rt.id AS task_raw_id,
    rt.task_id,
    rt.runset_id,
    runset.runset_id AS task_plan_id,
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
  LEFT JOIN hub_owner.req_runset runset
    ON runset.id = rt.runset_id
  WHERE runset.runset_id IN ('TP102','TP586','TP570')
),
sample_positions AS (
  SELECT
    tm.proc_exec_id,
    tm.task_raw_id,
    tm.task_id,
    tm.task_plan_id,
    tm.task_status,
    REGEXP_SUBSTR(tm.sample_list, '[^,]+', 1, LEVEL) AS sample_id,
    LEVEL - 1 AS sample_pos_zero_based,
    LEVEL AS sample_pos_one_based
  FROM task_map tm
  CONNECT BY REGEXP_SUBSTR(tm.sample_list, '[^,]+', 1, LEVEL) IS NOT NULL
         AND PRIOR tm.task_raw_id = tm.task_raw_id
         AND PRIOR SYS_GUID() IS NOT NULL
),
item_state_flags AS (
  SELECT
    pee.parent_id AS proc_exec_id,
    pee.id AS proc_elem_exec_id,
    pee.source_position,
    pee.process_number,
    pee.state AS elem_state,
    pee.item_states
  FROM hub_owner.pex_proc_elem_exec pee
  WHERE pee.item_states IS NOT NULL
)
SELECT
  sp.task_plan_id,
  sp.task_id,
  sp.sample_id,
  sp.sample_pos_zero_based,
  sp.sample_pos_one_based,
  isf.source_position,
  isf.process_number,
  isf.elem_state,
  isf.item_states,
  SUBSTR(isf.item_states, sp.sample_pos_one_based, 1) AS flag_at_sample_pos,
  CASE
    WHEN SUBSTR(isf.item_states, sp.sample_pos_one_based, 1) = 'X' THEN 'abandoned'
    WHEN SUBSTR(isf.item_states, sp.sample_pos_one_based, 1) = 'D' THEN 'completed'
    ELSE sp.task_status
  END AS derived_from_this_item_state
FROM sample_positions sp
LEFT JOIN item_state_flags isf
  ON isf.proc_exec_id = sp.proc_exec_id
WHERE sp.sample_id IN (
  'S001056','S001053','S001069','S001072',
  'S007887'
)
   OR sp.task_plan_id IN ('TP102','TP586','TP570')
ORDER BY
  sp.task_plan_id,
  sp.sample_pos_one_based,
  sp.sample_id,
  isf.source_position,
  isf.process_number;