WITH task_map AS (
  SELECT
    pe.id AS proc_exec_id,
    rt.id AS task_raw_id,
    rt.task_id,
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
    pee.source_position,
    pee.item_states
  FROM hub_owner.pex_proc_exec pe
  JOIN hub_owner.pex_proc_elem_exec pee
    ON pee.parent_id = pe.id
  WHERE pee.item_states IS NOT NULL
)
SELECT
  sp.sample_id,
  sp.task_id,
  sp.task_status,
  isf.source_position,
  isf.item_states,
  sp.sample_pos,
  SUBSTR(isf.item_states, sp.sample_pos + 1, 1) AS sample_flag,
  CASE
    WHEN SUBSTR(isf.item_states, sp.sample_pos + 1, 1) = 'X' THEN 'abandoned'
    WHEN SUBSTR(isf.item_states, sp.sample_pos + 1, 1) = 'D' THEN 'completed'
    ELSE 'no per-sample flag'
  END AS derived_sample_status
FROM sample_positions sp
LEFT JOIN item_state_flags isf
  ON isf.proc_exec_id = sp.proc_exec_id
WHERE sp.sample_id IN (
  'S007162','S007165','S007168','S007171','S007174',
  'S007178','S007181','S007184','S007187','S007190',
  'S005460'
)
ORDER BY sp.sample_id, isf.source_position;