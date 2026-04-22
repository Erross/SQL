WITH target_samples AS (
    SELECT 'S007162' AS sample_id FROM dual UNION ALL
    SELECT 'S007165' FROM dual UNION ALL
    SELECT 'S007168' FROM dual UNION ALL
    SELECT 'S007171' FROM dual UNION ALL
    SELECT 'S007174' FROM dual UNION ALL
    SELECT 'S007178' FROM dual UNION ALL
    SELECT 'S007181' FROM dual UNION ALL
    SELECT 'S007184' FROM dual UNION ALL
    SELECT 'S007187' FROM dual UNION ALL
    SELECT 'S007190' FROM dual
),
base AS (
    SELECT
        s.sample_id,
        s.name AS sample_name,
        s.id   AS sample_raw_id,

        rt.id  AS task_raw_id,
        rt.task_id,
        rt.work_item,
        rt.sample_list,
        rt.life_cycle_state AS task_status,

        pe.id  AS proc_exec_raw_id,
        pee.id AS proc_elem_exec_raw_id,
        pee.source_position  AS pee_source_position,
        pee.process_number   AS pee_process_number,
        pee.state            AS pee_state,
        pee.item_states      AS pee_item_states,

        peep.id AS peep_id,
        peep.source_position AS peep_source_position,
        peep.type            AS peep_type,

        ctx.id       AS context_id,
        ctx.context  AS retrieval_context,

        meas_s.id            AS meas_sample_id,
        meas_s.row_index     AS meas_row_index,
        meas_s.measurement_id,
        meas_s.last_updated  AS meas_sample_last_updated,

        m.record_name,
        m.record_date,
        m.context            AS measurement_context,
        m.measurement_type,
        m.info,
        m.raw_data,
        m.last_updated       AS measurement_last_updated,

        pv.id                AS pv_id,
        pv.parent_identity   AS pv_parent_identity,
        pv.item_index,
        pv.group_index,
        pv.value_key,
        pv.value_type,
        pv.value_string,
        pv.value_numeric,
        pv.value_text,
        pv.value_numeric_text,
        pv.interpretation,
        pv.last_updated      AS pv_last_updated,

        (
            SELECT COUNT(DISTINCT ms2.row_index)
            FROM hub_owner.res_measurementsample ms2
            WHERE ms2.context_id = meas_s.context_id
              AND ms2.row_index < meas_s.row_index
        ) AS derived_item_index_old,

        ROW_NUMBER() OVER (
            PARTITION BY meas_s.context_id
            ORDER BY meas_s.row_index, meas_s.id
        ) - 1 AS derived_item_index_ctx_only,

        ROW_NUMBER() OVER (
            PARTITION BY meas_s.context_id, meas_s.measurement_id
            ORDER BY meas_s.row_index, meas_s.id
        ) - 1 AS derived_item_index_ctx_meas

    FROM hub_owner.sam_sample s
    JOIN target_samples ts
      ON ts.sample_id = s.sample_id

    JOIN hub_owner.res_measurementsample meas_s
      ON meas_s.mapped_sample_id = s.id

    JOIN hub_owner.res_retrieval_context ctx
      ON ctx.id = meas_s.context_id

    JOIN hub_owner.res_measurement m
      ON m.id = meas_s.measurement_id

    JOIN hub_owner.pex_proc_elem_exec pee
      ON ctx.context =
         'urn:pexelement:' ||
         LOWER(
             SUBSTR(RAWTOHEX(pee.id),1,8)||'-'||
             SUBSTR(RAWTOHEX(pee.id),9,4)||'-'||
             SUBSTR(RAWTOHEX(pee.id),13,4)||'-'||
             SUBSTR(RAWTOHEX(pee.id),17,4)||'-'||
             SUBSTR(RAWTOHEX(pee.id),21,12)
         )

    JOIN hub_owner.pex_proc_exec pe
      ON pe.id = pee.parent_id

    JOIN hub_owner.pex_proc_elem_exec_param peep
      ON peep.parent_id = pee.id

    LEFT JOIN hub_owner.cor_parameter_value pv
      ON pv.parent_identity = peep.id

    LEFT JOIN hub_owner.req_task rt
      ON rt.work_item LIKE '%' || LOWER(
             SUBSTR(RAWTOHEX(pe.id),1,8)||'-'||
             SUBSTR(RAWTOHEX(pe.id),9,4)||'-'||
             SUBSTR(RAWTOHEX(pe.id),13,4)||'-'||
             SUBSTR(RAWTOHEX(pe.id),17,4)||'-'||
             SUBSTR(RAWTOHEX(pe.id),21,12)
         ) || '%'
     AND INSTR(',' || rt.sample_list || ',', ',' || s.sample_id || ',') > 0
)
SELECT
    sample_id,
    sample_name,

    task_id,
    task_status,

    meas_row_index,
    derived_item_index_old,
    derived_item_index_ctx_only,
    derived_item_index_ctx_meas,

    item_index,
    group_index,

    record_name,
    record_date,
    measurement_type,

    peep_source_position,
    peep_type,
    pv_id,
    value_key,
    value_type,
    value_string,
    value_numeric,
    value_text,
    value_numeric_text,
    interpretation,

    RAWTOHEX(context_id)          AS context_id_hex,
    RAWTOHEX(measurement_id)      AS measurement_id_hex,
    RAWTOHEX(proc_exec_raw_id)    AS proc_exec_id_hex,
    RAWTOHEX(proc_elem_exec_raw_id) AS proc_elem_exec_id_hex,
    RAWTOHEX(peep_id)             AS peep_id_hex,
    RAWTOHEX(pv_parent_identity)  AS pv_parent_identity_hex,

    retrieval_context,
    measurement_context,
    info,
    raw_data,

    pv_last_updated,
    measurement_last_updated,
    meas_sample_last_updated

FROM base
WHERE value_string IS NOT NULL
   OR value_numeric IS NOT NULL
   OR value_text IS NOT NULL
ORDER BY
    context_id,
    measurement_id,
    peep_source_position,
    NVL(group_index, -1),
    NVL(item_index, -1),
    sample_id,
    pv_last_updated;