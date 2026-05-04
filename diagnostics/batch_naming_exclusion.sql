SELECT
    rs.runset_id,
    rs.id AS runset_raw_id,
    rs.date_created AS runset_created,
    proj.name AS project_name,

    rt.id AS task_raw_id,
    rt.task_id,
    rt.task_name,
    rt.life_cycle_state AS task_status,
    rt.deleted,
    rt.sample_list,
    rt.work_item

FROM hub_owner.req_runset rs
LEFT JOIN hub_owner.res_project proj
    ON proj.id = rs.project_id
LEFT JOIN hub_owner.req_task rt
    ON rt.runset_id = rs.id
   AND NVL(rt.deleted, 'x') <> 'Y'

WHERE rs.runset_id IN ('TP247', 'TP009')

ORDER BY
    rs.runset_id,
    rt.task_name,
    rt.task_id;

    --sample order debug
WITH
task_map AS (
    SELECT
        pe.id AS proc_exec_id,
        rt.id AS task_raw_id,
        rt.task_id,
        rt.task_name,
        rt.runset_id,
        rt.sample_list,
        rt.work_item,
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
    JOIN hub_owner.req_runset rs
        ON rs.id = rt.runset_id
    WHERE rs.runset_id = 'TP009'
      AND rt.task_id = 'T039'
      AND NVL(rt.deleted, 'x') <> 'Y'
),

task_samples AS (
    SELECT
        tm.proc_exec_id,
        tm.task_raw_id,
        tm.task_id,
        tm.task_name,
        n.lvl - 1 AS task_item_index,
        REGEXP_SUBSTR(tm.sample_list, '[^,]+', 1, n.lvl) AS task_sample_id
    FROM task_map tm
    JOIN (
        SELECT LEVEL AS lvl
        FROM dual
        CONNECT BY LEVEL <= 20
    ) n
        ON n.lvl <= REGEXP_COUNT(tm.sample_list, ',') + 1
    WHERE REGEXP_SUBSTR(tm.sample_list, '[^,]+', 1, n.lvl) IS NOT NULL
),

param_rows AS (
    SELECT
        tm.proc_exec_id,
        tm.task_id,
        tm.task_name,
        tm.sample_list,
        pee.id AS proc_elem_exec_id,
        peep.id AS peep_id,
        peep.source_position,
        pv.value_key,
        pv.value_type,
        pv.item_index,
        NVL(pv.group_index, 1) AS group_index,
        pv.value_string,
        pv.value_text,
        pv.value_numeric_text,
        pv.value_numeric,
        pv.interpretation,
        pv.last_updated
    FROM task_map tm
    JOIN hub_owner.pex_proc_elem_exec pee
        ON pee.parent_id = tm.proc_exec_id
    JOIN hub_owner.pex_proc_elem_exec_param peep
        ON peep.parent_id = pee.id
    JOIN hub_owner.cor_parameter_value pv
        ON pv.parent_identity = peep.id
),

field_defs AS (
    SELECT
        peep_id,
        source_position,
        LOWER(TRIM(MAX(CASE
            WHEN value_key = 'AE'
             AND value_type = 'Equipment'
            THEN value_string
        END))) AS field_name
    FROM param_rows
    WHERE value_key = 'AE'
    GROUP BY
        peep_id,
        source_position
),

field_values AS (
    SELECT
        pr.proc_exec_id,
        pr.task_id,
        pr.task_name,
        pr.sample_list,
        pr.proc_elem_exec_id,
        pr.peep_id,
        pr.source_position,
        fd.field_name,
        pr.item_index,
        pr.group_index,
        pr.value_string,
        pr.value_text,
        pr.value_numeric_text,
        pr.value_numeric,
        pr.interpretation,
        pr.last_updated
    FROM param_rows pr
    JOIN field_defs fd
        ON fd.peep_id = pr.peep_id
    WHERE pr.value_key = 'A'
),

context_map AS (
    SELECT
        fv.*,
        ctx.id AS context_id
    FROM field_values fv
    LEFT JOIN hub_owner.res_retrieval_context ctx
        ON ctx.context = 'urn:pexelement:' || LOWER(
            SUBSTR(RAWTOHEX(fv.proc_elem_exec_id),1,8)||'-'||
            SUBSTR(RAWTOHEX(fv.proc_elem_exec_id),9,4)||'-'||
            SUBSTR(RAWTOHEX(fv.proc_elem_exec_id),13,4)||'-'||
            SUBSTR(RAWTOHEX(fv.proc_elem_exec_id),17,4)||'-'||
            SUBSTR(RAWTOHEX(fv.proc_elem_exec_id),21,12)
        )
),

rms_map AS (
    SELECT
        rms.context_id,
        rms.id AS measurementsample_id,
        rms.measurement_id,
        rms.row_index,
        rms.sample_id AS instrument_sample_id,
        s.sample_id AS mapped_sample_id,
        s.name AS mapped_sample_name
    FROM hub_owner.res_measurementsample rms
    JOIN hub_owner.sam_sample s
        ON s.id = rms.mapped_sample_id
)

SELECT
    cm.task_id,
    cm.task_name,
    cm.sample_list,

    cm.proc_elem_exec_id,
    cm.source_position,
    cm.field_name,
    cm.item_index AS equipment_item_index,
    cm.group_index,

    cm.value_string,
    cm.value_text,
    cm.value_numeric_text,
    cm.value_numeric,
    cm.interpretation,
    cm.last_updated,

    ts.task_item_index,
    ts.task_sample_id,
    s_task.name AS task_sample_name,

    rms.row_index AS rms_row_index,
    rms.instrument_sample_id,
    rms.mapped_sample_id AS rms_mapped_sample_id,
    rms.mapped_sample_name,
    rms.measurementsample_id,
    rms.measurement_id

FROM context_map cm
LEFT JOIN task_samples ts
    ON ts.proc_exec_id = cm.proc_exec_id
   AND ts.task_id = cm.task_id
   AND ts.task_item_index = cm.item_index
LEFT JOIN hub_owner.sam_sample s_task
    ON s_task.sample_id = ts.task_sample_id
LEFT JOIN rms_map rms
    ON rms.context_id = cm.context_id
   AND rms.row_index = cm.item_index

WHERE cm.field_name IN (
    'sample_id',
    'ov_meter_reading',
    'data_id',
    'meter_number',
    'meter number',
    'sampling_point_time',
    'sampling point time',
    'sampling point time *'
)

ORDER BY
    cm.proc_elem_exec_id,
    cm.item_index,
    cm.group_index,
    cm.source_position,
    cm.field_name;