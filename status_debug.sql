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

  //2nd query

  WITH task_map AS (
  SELECT
    pe.id AS proc_exec_id,
    rt.id AS task_raw_id,
    rt.task_id,
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
  JOIN hub_owner.req_runset runset
    ON runset.id = rt.runset_id
  WHERE runset.runset_id IN ('TP102','TP570','TP586')
),
pee_states AS (
  SELECT
    tm.task_plan_id,
    tm.task_id,
    tm.proc_exec_id,
    pee.id AS proc_elem_exec_id,
    pee.source_position,
    pee.process_number,
    pee.state,
    pee.item_states,
    LENGTH(pee.item_states) AS item_states_len
  FROM task_map tm
  JOIN hub_owner.pex_proc_elem_exec pee
    ON pee.parent_id = tm.proc_exec_id
  WHERE pee.item_states IS NOT NULL
)
SELECT
  ps.task_plan_id,
  ps.task_id,
  ps.source_position,
  ps.process_number,
  ps.state,
  ps.item_states,
  ps.item_states_len,
  COUNT(peep.id) AS param_count
FROM pee_states ps
LEFT JOIN hub_owner.pex_proc_elem_exec_param peep
  ON peep.parent_id = ps.proc_elem_exec_id
GROUP BY
  ps.task_plan_id,
  ps.task_id,
  ps.source_position,
  ps.process_number,
  ps.state,
  ps.item_states,
  ps.item_states_len
ORDER BY
  ps.task_plan_id,
  ps.task_id,
  ps.source_position,
  ps.process_number;

  //3rd check

WITH task_map AS (
  SELECT
    pe.id AS proc_exec_id,
    rt.id AS task_raw_id,
    rt.task_id,
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
  JOIN hub_owner.req_runset runset
    ON runset.id = rt.runset_id
  WHERE runset.runset_id IN ('TP102','TP570','TP586')
),
task_params AS (
  SELECT
    tm.task_plan_id,
    tm.task_id,
    tm.proc_exec_id,
    rtp.task_id AS rtp_task_raw_id,
    p.id AS cor_parameter_id,
    p.name AS parameter_name,
    p.display_name AS parameter_display_name,
    p.value_type
  FROM task_map tm
  JOIN hub_owner.req_task_parameter rtp
    ON rtp.task_id = tm.task_raw_id
  JOIN hub_owner.cor_parameter p
    ON p.id = rtp.parameter_id
),
pee_states AS (
  SELECT
    tm.task_plan_id,
    tm.task_id,
    tm.proc_exec_id,
    pee.id AS proc_elem_exec_id,
    pee.source_position,
    pee.process_number,
    pee.state,
    pee.item_states,
    LENGTH(pee.item_states) AS item_states_len
  FROM task_map tm
  JOIN hub_owner.pex_proc_elem_exec pee
    ON pee.parent_id = tm.proc_exec_id
  WHERE pee.item_states IS NOT NULL
),
pee_params AS (
  SELECT
    ps.task_plan_id,
    ps.task_id,
    ps.proc_exec_id,
    ps.proc_elem_exec_id,
    ps.source_position,
    ps.process_number,
    ps.state,
    ps.item_states,
    ps.item_states_len,
    peep.id AS peep_id,
    peep.source_position AS peep_source_position,
    peep.type AS peep_type,
    pv.parent_identity,
    pv.item_index,
    pv.value_key,
    pv.value_type AS pv_value_type,
    pv.value_string,
    pv.value_numeric,
    pv.value_text,
    pv.value_numeric_text
  FROM pee_states ps
  LEFT JOIN hub_owner.pex_proc_elem_exec_param peep
    ON peep.parent_id = ps.proc_elem_exec_id
  LEFT JOIN hub_owner.cor_parameter_value pv
    ON pv.parent_identity = peep.id
)
SELECT
  pp.task_plan_id,
  pp.task_id,
  pp.source_position,
  pp.process_number,
  pp.state,
  pp.item_states,
  pp.item_states_len,
  pp.peep_source_position,
  pp.peep_type,
  pp.item_index,
  pp.value_key,
  pp.pv_value_type,
  pp.value_string,
  pp.value_numeric,
  pp.value_text,
  pp.value_numeric_text
FROM pee_params pp
WHERE pp.item_states LIKE '%X%'
   OR pp.item_states LIKE '%D%'
ORDER BY
  pp.task_plan_id,
  pp.task_id,
  pp.source_position,
  pp.peep_source_position,
  pp.item_index;