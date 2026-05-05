WITH
/*
    Query_with_OV_File - fixed machine-result mapping

    Known fixes included:
      1. REQ_TASK.TASK_NAME is carried in task_map for diagnostics/future filtering.
      2. Machine rows require real process context: project name and project plan must resolve.
         This removes TP247 / QAP_OVEN_PROCESS style rows without filtering out planned samples generally.
      3. Sample ID is cardinal. The instrument/packet sample_id can be manually changed and is not trusted
         for assigning the business/result sample. Equipment rows now prefer global task sample-list position first, so combined equipment packets across sibling tasks map to the Biovia Sample ID (Name) column.
      4. RES_MEASUREMENTSAMPLE.row_index is second fallback. Packet/instrument sample_id is last fallback only.
      5. CSV/file fallback rows are also gated to a valid process context so helper/batch measurements do not leak back in.
*/

task_map_base AS (
    SELECT
        pe.id AS proc_exec_id,
        rt.id AS task_raw_id,
        rt.task_id,
        rt.task_name,
        rt.runset_id,
        rt.sample_list,
        rt.life_cycle_state AS task_status,
        REGEXP_COUNT(rt.sample_list, ',') + 1 AS sample_count
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
      AND NVL(rt.deleted, 'x') <> 'Y'
      AND NVL(UPPER(rt.task_name), 'x') NOT LIKE 'OV_BATCH%'
),

task_map AS (
    SELECT
        tmb.*,
        NVL(
            SUM(tmb.sample_count) OVER (
                PARTITION BY tmb.proc_exec_id
                ORDER BY tmb.task_id, tmb.task_raw_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ),
            0
        ) AS base_item_index
    FROM task_map_base tmb
),

task_samples AS (
    SELECT
        tm.proc_exec_id,
        tm.task_raw_id,
        tm.task_id,
        tm.task_name,
        tm.runset_id,
        tm.task_status,
        n.lvl - 1 AS item_index,

        /*
            Local item_index is used for manual task parameters.
            global_item_index is used for equipment packets that are stored as one combined
            packet across multiple sibling tasks in the same process execution.
        */
        tm.base_item_index + n.lvl - 1 AS global_item_index,

        REGEXP_SUBSTR(tm.sample_list, '[^,]+', 1, n.lvl) AS sample_id
    FROM task_map tm
    JOIN (
        SELECT LEVEL AS lvl
        FROM dual
        CONNECT BY LEVEL <= 300
    ) n
        ON n.lvl <= REGEXP_COUNT(tm.sample_list, ',') + 1
    WHERE REGEXP_SUBSTR(tm.sample_list, '[^,]+', 1, n.lvl) IS NOT NULL
),

valid_process_samples AS (
    SELECT DISTINCT
        sample_id
    FROM task_samples
),

item_state_sample_flags AS (
    SELECT
        pee.parent_id AS proc_exec_id,
        s.sample_id,
        CASE
            WHEN MAX(CASE WHEN SUBSTR(pee.item_states, pv.item_index + 1, 1) = 'X' THEN 1 ELSE 0 END) = 1
                THEN 'abandoned'
            WHEN MAX(CASE WHEN SUBSTR(pee.item_states, pv.item_index + 1, 1) = 'D' THEN 1 ELSE 0 END) = 1
                THEN 'completed'
            ELSE NULL
        END AS item_state_status
    FROM hub_owner.pex_proc_elem_exec pee
    JOIN hub_owner.pex_proc_elem_exec_param peep
        ON peep.parent_id = pee.id
    JOIN hub_owner.cor_parameter_value pv
        ON pv.parent_identity = peep.id
    JOIN hub_owner.sam_sample s
        ON s.name = COALESCE(
            pv.value_text,
            pv.value_string,
            pv.value_numeric_text,
            TO_CHAR(pv.value_numeric)
        )
    WHERE pee.item_states IS NOT NULL
      AND pv.value_key = 'A'
      AND pv.item_index IS NOT NULL
    GROUP BY
        pee.parent_id,
        s.sample_id
),

sample_exec_status AS (
    SELECT
        ts.proc_exec_id,
        ts.task_raw_id,
        ts.item_index,
        ts.sample_id,
        COALESCE(
            issf.item_state_status,
            ts.task_status,
            s.life_cycle_state
        ) AS derived_status
    FROM task_samples ts
    JOIN hub_owner.sam_sample s
        ON s.sample_id = ts.sample_id
    LEFT JOIN item_state_sample_flags issf
        ON issf.proc_exec_id = ts.proc_exec_id
       AND issf.sample_id = ts.sample_id
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
    WHERE pv.value_key = 'AE'
    GROUP BY
        pe.id,
        pee.id,
        peep.id,
        peep.source_position
),

equipment_param_values AS (
    SELECT
        pe.id AS proc_exec_id,
        pee.id AS proc_elem_exec_id,
        peep.id AS peep_id,
        peep.source_position,
        pv.item_index,
        NVL(pv.group_index, 1) AS group_index,

        MAX(CASE WHEN pv.value_key = 'A' THEN pv.value_string END) AS value_string,
        MAX(CASE WHEN pv.value_key = 'A' THEN pv.value_numeric END) AS value_numeric,
        MAX(CASE WHEN pv.value_key = 'A' THEN pv.value_text END) AS value_text,
        MAX(CASE WHEN pv.value_key = 'A' THEN pv.value_numeric_text END) AS value_numeric_text,
        MAX(CASE WHEN pv.value_key = 'A' THEN pv.interpretation END) AS interpretation,
        MAX(CASE WHEN pv.value_key = 'A' THEN pv.last_updated END) AS last_updated,
        MAX(CASE WHEN pv.value_key = 'A' THEN pv.unit END) AS unit_id

    FROM hub_owner.pex_proc_exec pe
    JOIN hub_owner.pex_proc_elem_exec pee
        ON pee.parent_id = pe.id
    JOIN hub_owner.pex_proc_elem_exec_param peep
        ON peep.parent_id = pee.id
    JOIN hub_owner.cor_parameter_value pv
        ON pv.parent_identity = peep.id
    WHERE pv.value_key = 'A'
    GROUP BY
        pe.id,
        pee.id,
        peep.id,
        peep.source_position,
        pv.item_index,
        NVL(pv.group_index, 1)
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

equipment_selected_result AS (
    SELECT
        ec.proc_exec_id,
        ec.proc_elem_exec_id,
        ec.item_index,
        ec.group_index,

        MAX(ec.field_name) KEEP (
            DENSE_RANK FIRST ORDER BY
                CASE
                    WHEN ec.value_numeric IS NOT NULL THEN 1
                    WHEN ec.value_numeric_text IS NOT NULL THEN 2
                    WHEN ec.value_text IS NOT NULL THEN 3
                    WHEN ec.value_string IS NOT NULL THEN 4
                    ELSE 9
                END,
                ec.source_position,
                ec.field_name
        ) AS field_name,

        MAX(ec.value_numeric) KEEP (
            DENSE_RANK FIRST ORDER BY
                CASE
                    WHEN ec.value_numeric IS NOT NULL THEN 1
                    WHEN ec.value_numeric_text IS NOT NULL THEN 2
                    WHEN ec.value_text IS NOT NULL THEN 3
                    WHEN ec.value_string IS NOT NULL THEN 4
                    ELSE 9
                END,
                ec.source_position,
                ec.field_name
        ) AS value_numeric,

        MAX(ec.value_numeric_text) KEEP (
            DENSE_RANK FIRST ORDER BY
                CASE
                    WHEN ec.value_numeric IS NOT NULL THEN 1
                    WHEN ec.value_numeric_text IS NOT NULL THEN 2
                    WHEN ec.value_text IS NOT NULL THEN 3
                    WHEN ec.value_string IS NOT NULL THEN 4
                    ELSE 9
                END,
                ec.source_position,
                ec.field_name
        ) AS value_numeric_text,

        MAX(ec.value_text) KEEP (
            DENSE_RANK FIRST ORDER BY
                CASE
                    WHEN ec.value_numeric IS NOT NULL THEN 1
                    WHEN ec.value_numeric_text IS NOT NULL THEN 2
                    WHEN ec.value_text IS NOT NULL THEN 3
                    WHEN ec.value_string IS NOT NULL THEN 4
                    ELSE 9
                END,
                ec.source_position,
                ec.field_name
        ) AS value_text,

        MAX(ec.value_string) KEEP (
            DENSE_RANK FIRST ORDER BY
                CASE
                    WHEN ec.value_numeric IS NOT NULL THEN 1
                    WHEN ec.value_numeric_text IS NOT NULL THEN 2
                    WHEN ec.value_text IS NOT NULL THEN 3
                    WHEN ec.value_string IS NOT NULL THEN 4
                    ELSE 9
                END,
                ec.source_position,
                ec.field_name
        ) AS value_string,

        MAX(ec.interpretation) KEEP (
            DENSE_RANK FIRST ORDER BY
                CASE
                    WHEN ec.value_numeric IS NOT NULL THEN 1
                    WHEN ec.value_numeric_text IS NOT NULL THEN 2
                    WHEN ec.value_text IS NOT NULL THEN 3
                    WHEN ec.value_string IS NOT NULL THEN 4
                    ELSE 9
                END,
                ec.source_position,
                ec.field_name
        ) AS interpretation,

        MAX(ec.last_updated) KEEP (
            DENSE_RANK FIRST ORDER BY
                CASE
                    WHEN ec.value_numeric IS NOT NULL THEN 1
                    WHEN ec.value_numeric_text IS NOT NULL THEN 2
                    WHEN ec.value_text IS NOT NULL THEN 3
                    WHEN ec.value_string IS NOT NULL THEN 4
                    ELSE 9
                END,
                ec.source_position,
                ec.field_name
        ) AS last_updated,

        MAX(ec.unit_id) KEEP (
            DENSE_RANK FIRST ORDER BY
                CASE
                    WHEN ec.value_numeric IS NOT NULL THEN 1
                    WHEN ec.value_numeric_text IS NOT NULL THEN 2
                    WHEN ec.value_text IS NOT NULL THEN 3
                    WHEN ec.value_string IS NOT NULL THEN 4
                    ELSE 9
                END,
                ec.source_position,
                ec.field_name
        ) AS unit_id,

        MIN(ec.source_position) KEEP (
            DENSE_RANK FIRST ORDER BY
                CASE
                    WHEN ec.value_numeric IS NOT NULL THEN 1
                    WHEN ec.value_numeric_text IS NOT NULL THEN 2
                    WHEN ec.value_text IS NOT NULL THEN 3
                    WHEN ec.value_string IS NOT NULL THEN 4
                    ELSE 9
                END,
                ec.source_position,
                ec.field_name
        ) AS source_position

    FROM equipment_classified ec
    WHERE ec.field_role = 'CANDIDATE_RESULT'
      AND ec.value_numeric IS NOT NULL
      AND NOT (
             ec.field_name LIKE '%weight%'
          OR ec.field_name LIKE '%mass%'
          OR ec.field_name LIKE '%tare%'
          OR ec.field_name LIKE '%gross%'
          OR ec.field_name LIKE '%net_weight%'
          OR ec.field_name LIKE '%sample_weight%'
          OR ec.field_name LIKE '%dish_weight%'
      )
    GROUP BY
        ec.proc_exec_id,
        ec.proc_elem_exec_id,
        ec.item_index,
        ec.group_index
),

equipment_row_metadata AS (
    SELECT
        proc_exec_id,
        proc_elem_exec_id,
        item_index,
        group_index,
        MAX(CASE
            WHEN field_name = 'sample_id'
            THEN COALESCE(
                value_string,
                value_text,
                value_numeric_text,
                TO_CHAR(value_numeric)
            )
        END) AS packet_sample_id
    FROM equipment_classified
    GROUP BY
        proc_exec_id,
        proc_elem_exec_id,
        item_index,
        group_index
),

equipment_rows AS (
    SELECT
        m.proc_exec_id,
        m.proc_elem_exec_id,
        m.item_index,
        m.group_index,
        m.packet_sample_id,
        r.field_name,
        r.value_numeric,
        r.value_numeric_text,
        r.value_text,
        r.value_string,
        r.interpretation,
        r.last_updated,
        r.unit_id,
        r.source_position
    FROM equipment_row_metadata m
    JOIN equipment_selected_result r
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
        ON ctx.context = 'urn:pexelement:' || LOWER(
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
        meas_s.row_index AS derived_item_index,
        MAX(meas_s.mapped_sample_id) KEEP (
            DENSE_RANK FIRST ORDER BY meas_s.id
        ) AS mapped_sample_id
    FROM hub_owner.res_measurementsample meas_s
    WHERE meas_s.row_index IS NOT NULL
    GROUP BY
        meas_s.context_id,
        meas_s.row_index
),

equipment_resolved AS (
    SELECT
        ewc.proc_exec_id,
        ewc.proc_elem_exec_id,
        ewc.item_index,
        ewc.group_index,
        ewc.packet_sample_id,
        ewc.field_name,
        ewc.value_numeric,
        ewc.value_numeric_text,
        ewc.value_text,
        ewc.value_string,
        ewc.interpretation,
        ewc.last_updated,
        ewc.unit_id,
        ewc.source_position,

        /*
            Sample ID is cardinal for reporting.

            Important distinction:
              - item_index on manual task parameters is local to a REQ_TASK.
              - equipment packets can be stored as one combined packet across sibling
                tasks in the same process execution.

            Therefore the primary equipment mapping is:
              equipment item_index -> task_samples.global_item_index

            The packet/instrument sample_id can be manually edited, so it is only a
            last-resort fallback and must never override the business Sample ID.
        */
        COALESCE(
            s_fb_task.id,
            s_fb_idx.id,
            s_packet.id
        ) AS sample_raw_id,

        COALESCE(
            s_fb_task.sample_id,
            s_fb_idx.sample_id,
            s_packet.sample_id
        ) AS sample_id,

        COALESCE(
            ts_fb.task_raw_id,
            ts_idx.task_raw_id
        ) AS resolved_task_raw_id

    FROM equipment_with_context ewc

    /* 1. Primary mapping: global task sample position / business Sample ID. */
    LEFT JOIN task_samples ts_fb
        ON ts_fb.proc_exec_id = ewc.proc_exec_id
       AND ts_fb.global_item_index = ewc.item_index
    LEFT JOIN hub_owner.sam_sample s_fb_task
        ON s_fb_task.sample_id = ts_fb.sample_id

    /* 2. Secondary fallback: measurement-sample row index, only when global task-position mapping failed. */
    LEFT JOIN fallback_measurements fm_idx
        ON fm_idx.context_id = ewc.context_id
       AND fm_idx.derived_item_index = ewc.item_index
       AND s_fb_task.id IS NULL
    LEFT JOIN hub_owner.sam_sample s_fb_idx
        ON s_fb_idx.id = fm_idx.mapped_sample_id
       AND s_fb_task.id IS NULL

    /* Try to locate the task context for the RMS fallback sample without duplicating across all sibling tasks. */
    LEFT JOIN task_samples ts_idx
        ON ts_idx.proc_exec_id = ewc.proc_exec_id
       AND ts_idx.sample_id = s_fb_idx.sample_id
       AND s_fb_task.id IS NULL

    /* 3. Last fallback only: packet/instrument sample_id. */
    LEFT JOIN hub_owner.sam_sample s_packet
        ON s_packet.sample_id = ewc.packet_sample_id
       AND s_fb_task.id IS NULL
       AND s_fb_idx.id IS NULL
),

manual_results AS (
    SELECT
        s.name AS "Sample Name",
        s.sample_id AS "Sample ID",
        ses.derived_status AS "Sample Status",
        ms.sample_id AS "Master Sample ID",
        sp.sampling_point AS "Sampling point",

        TRIM(REGEXP_REPLACE(
            REPLACE(REPLACE(sp.sampling_point_description, CHR(13), ''), CHR(10), ''),
            '\s*\[[[:digit:]]+\]\s*$',
            ''
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
        ts.task_status AS "Task Status",
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
    JOIN task_samples ts
        ON ts.task_raw_id = rt.id
       AND ts.item_index = pv.item_index
    JOIN sample_exec_status ses
        ON ses.proc_exec_id = ts.proc_exec_id
       AND ses.task_raw_id = ts.task_raw_id
       AND ses.item_index = ts.item_index
    JOIN hub_owner.sam_sample s
        ON s.sample_id = ts.sample_id
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
      AND NVL(ms.sample_id, 'x') != 'planned'
      AND p.display_name != 'Sample'
      AND p.value_type NOT IN ('Vocabulary')
      AND pv.value_string IS NOT NULL
      AND cs.id = '5FD74EE88C024C2EB908BCE0E176B0E8'
),

equipment_results_raw AS (
    SELECT
        s.name AS "Sample Name",
        s.sample_id AS "Sample ID",
        ses.derived_status AS "Sample Status",
        ms.sample_id AS "Master Sample ID",
        sp.sampling_point AS "Sampling point",

        TRIM(REGEXP_REPLACE(
            REPLACE(REPLACE(sp.sampling_point_description, CHR(13), ''), CHR(10), ''),
            '\s*\[[[:digit:]]+\]\s*$',
            ''
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
        COALESCE(
            er.value_numeric_text,
            er.value_text,
            er.value_string,
            TO_CHAR(er.value_numeric)
        ) AS "Formatted result",
        er.last_updated AS "Result entered",
        'EQUIPMENT' AS "Result Source",
        uom.description AS "UOM",
        rp.tp_project_plan AS "Task Plan Project Plan"

    FROM equipment_resolved er
    JOIN task_map tm
        ON tm.task_raw_id = er.resolved_task_raw_id
    JOIN sample_exec_status ses
        ON ses.sample_id = er.sample_id
       AND ses.proc_exec_id = tm.proc_exec_id
       AND ses.task_raw_id = tm.task_raw_id
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
      AND NVL(ms.sample_id, 'x') != 'planned'
      AND er.value_numeric IS NOT NULL
      AND er.sample_raw_id IS NOT NULL
      AND proj.name IS NOT NULL
      AND rp.tp_project_plan IS NOT NULL
),

equipment_results AS (
    SELECT
        "Sample Name",
        "Sample ID",
        "Sample Status",
        "Master Sample ID",
        "Sampling point",
        "Sampling point description",
        "LINE-1",
        "Owner",
        "Product Code",
        "Product Description",
        "CIG_PRODUCT_CODE",
        "CIG_PRODUCT_DESCRIPTION",
        "Spec_Group",
        "Task Plan Project",
        "Task Plan ID",
        "Task Plan Creation Date",
        "Task Status",
        "Characteristic",
        "Result",
        "Formatted result",
        "Result entered",
        "Result Source",
        "UOM",
        "Task Plan Project Plan"
    FROM equipment_results_raw
    GROUP BY
        "Sample Name",
        "Sample ID",
        "Sample Status",
        "Master Sample ID",
        "Sampling point",
        "Sampling point description",
        "LINE-1",
        "Owner",
        "Product Code",
        "Product Description",
        "CIG_PRODUCT_CODE",
        "CIG_PRODUCT_DESCRIPTION",
        "Spec_Group",
        "Task Plan Project",
        "Task Plan ID",
        "Task Plan Creation Date",
        "Task Status",
        "Characteristic",
        "Result",
        "Formatted result",
        "Result entered",
        "Result Source",
        "UOM",
        "Task Plan Project Plan"
),

existing_equipment_ov_samples AS (
    SELECT
        "Sample ID"
    FROM equipment_results
    WHERE LOWER("Characteristic") = 'ov_meter_reading'
    GROUP BY "Sample ID"
),

equipment_file_packet_scope AS (
    SELECT
        s.id AS sample_raw_id,
        s.name AS sample_name,
        s.sample_id AS sample_id,

        rms.id AS measurementsample_raw_id,
        rms.sample_id AS instrument_sample_id,
        rms.row_index,
        rms.context_id,

        rm.id AS measurement_raw_id,
        rm.record_name,
        CASE
            WHEN INSTR(rm.record_name, '/') > 0
            THEN SUBSTR(rm.record_name, INSTR(rm.record_name, '/', -1) + 1)
        END AS record_basename_after_slash,
        rm.record_date,
        rm.date_created AS measurement_created,
        rm.last_updated AS measurement_last_updated,
        rm.equipment_id,
        rm.equipment_work_item_id,

        et.name AS equipment_type_name,
        dp.id AS data_packet_id,
        dp.name AS data_packet_name

    FROM hub_owner.sam_sample s
    JOIN valid_process_samples vps
        ON vps.sample_id = s.sample_id
    JOIN hub_owner.res_measurementsample rms
        ON rms.mapped_sample_id = s.id
    JOIN hub_owner.res_measurement rm
        ON rm.id = rms.measurement_id
    JOIN hub_owner.res_equipment e
        ON e.id = rm.equipment_id
    JOIN hub_owner.res_equipment_type et
        ON et.id = e.equipment_type_id
    JOIN hub_owner.res_data_packet dp
        ON dp.id = et.data_packet_id
    JOIN hub_owner.res_data_field df_ov
        ON df_ov.data_packet_id = dp.id
       AND UPPER(REPLACE(df_ov.name, ' ', '_')) = 'OV_METER_READING'
    LEFT JOIN existing_equipment_ov_samples existing_ov
        ON existing_ov."Sample ID" = s.sample_id
    WHERE rm.measurement_type = 'Data Packet'
      AND rm.record_name IS NOT NULL
      AND LOWER(rm.record_name) LIKE '%.csv'
      AND rms.sample_id IS NOT NULL
      AND rms.sample_id <> s.sample_id
      AND existing_ov."Sample ID" IS NULL
),

equipment_file_text AS (
    SELECT
        ps.*,
        fm.id AS file_metadata_id,
        fm.name AS file_name,
        fm.file_size,
        fm.date_created AS file_date_created,
        fm.last_updated AS file_last_updated,

        UTL_RAW.CAST_TO_VARCHAR2(
            DBMS_LOB.SUBSTR(fc.content, 4000, 1)
        ) AS file_text

    FROM equipment_file_packet_scope ps
    JOIN hub_owner.file_metadata fm
        ON fm.storage_type = 'DATABASE'
       AND (
              fm.name = ps.record_name
           OR (
                  ps.record_basename_after_slash IS NOT NULL
              AND fm.name = ps.record_basename_after_slash
           )
       )
    JOIN hub_owner.file_content fc
        ON fc.file_id = fm.id
    WHERE DBMS_LOB.GETLENGTH(fc.content) <= 4000
),

equipment_file_values_raw AS (
    SELECT
        eft.sample_raw_id,
        eft.sample_name,
        eft.sample_id,
        eft.instrument_sample_id,
        eft.measurementsample_raw_id,
        eft.measurement_raw_id,
        eft.record_name,
        eft.record_date,
        eft.measurement_created,
        eft.measurement_last_updated,
        eft.file_metadata_id,
        eft.file_name,
        eft.file_size,
        eft.file_date_created,
        eft.file_last_updated,
        REGEXP_SUBSTR(
            eft.file_text,
            'Data,[^,]*,([^,]*),' || eft.instrument_sample_id || ',',
            1, 1, 'i', 1
        ) AS csv_data_id,
        eft.instrument_sample_id AS csv_sample_id,
        REGEXP_SUBSTR(
            eft.file_text,
            'Data,[^,]*,[^,]*,' || eft.instrument_sample_id || ',([^,]*),',
            1, 1, 'i', 1
        ) AS csv_meter_number,
        REGEXP_SUBSTR(
            eft.file_text,
            'Data,[^,]*,[^,]*,' || eft.instrument_sample_id || ',[^,]*,(-?[0-9]+(\.[0-9]+)?)',
            1, 1, 'i', 1
        ) AS csv_ov_meter_reading,
        CAST(NULL AS VARCHAR2(255)) AS csv_sampling_point_time
    FROM equipment_file_text eft
    WHERE REGEXP_LIKE(
        eft.file_text,
        'Data,[^,]*,[^,]*,' || eft.instrument_sample_id || ',[^,]*,-?[0-9]+(\.[0-9]+)?',
        'i'
    )
),

equipment_file_values AS (
    SELECT
        sample_raw_id,
        sample_name,
        sample_id,
        instrument_sample_id,
        measurementsample_raw_id,
        measurement_raw_id,
        record_name,
        record_date,
        measurement_created,
        measurement_last_updated,
        csv_data_id,
        csv_sample_id,
        csv_meter_number,
        csv_sampling_point_time,
        TO_NUMBER(
            csv_ov_meter_reading,
            '9999999990D999999',
            'NLS_NUMERIC_CHARACTERS=.,'
        ) AS ov_meter_reading
    FROM equipment_file_values_raw
    WHERE REGEXP_LIKE(csv_ov_meter_reading, '^-?[0-9]+(\.[0-9]+)?$')
    GROUP BY
        sample_raw_id,
        sample_name,
        sample_id,
        instrument_sample_id,
        measurementsample_raw_id,
        measurement_raw_id,
        record_name,
        record_date,
        measurement_created,
        measurement_last_updated,
        csv_data_id,
        csv_sample_id,
        csv_meter_number,
        csv_sampling_point_time,
        csv_ov_meter_reading
),

equipment_file_values_contexted AS (
    SELECT *
    FROM (
        SELECT
            f.*,
            proj.name AS context_project_name,
            rs.runset_id AS context_task_plan_id,
            rs.date_created AS context_task_plan_creation_date,
            ts.task_status AS context_task_status,
            rp.tp_project_plan AS context_task_plan_project_plan,
            ROW_NUMBER() OVER (
                PARTITION BY
                    f.measurementsample_raw_id,
                    f.measurement_raw_id
                ORDER BY
                    CASE
                        WHEN rs.date_created <= NVL(f.measurement_last_updated, SYSDATE) THEN 0
                        ELSE 1
                    END,
                    rs.date_created DESC,
                    ts.task_id
            ) AS context_rn
        FROM equipment_file_values f
        JOIN task_samples ts
            ON ts.sample_id = f.sample_id
        JOIN hub_owner.req_runset rs
            ON rs.id = ts.runset_id
        JOIN hub_owner.res_project proj
            ON proj.id = rs.project_id
        JOIN runset_properties rp
            ON rp.runset_raw_id = rs.id
        WHERE proj.name IS NOT NULL
          AND rp.tp_project_plan IS NOT NULL
    )
    WHERE context_rn = 1
),

equipment_file_results AS (
    SELECT
        s.name AS "Sample Name",
        s.sample_id AS "Sample ID",
        s.life_cycle_state AS "Sample Status",
        ms.sample_id AS "Master Sample ID",
        sp.sampling_point AS "Sampling point",

        TRIM(REGEXP_REPLACE(
            REPLACE(REPLACE(sp.sampling_point_description, CHR(13), ''), CHR(10), ''),
            '\s*\[[[:digit:]]+\]\s*$',
            ''
        )) AS "Sampling point description",

        sp.line AS "LINE-1",
        usr.name AS "Owner",
        sp.product_code AS "Product Code",
        sp.product_description AS "Product Description",
        sp.cig_product_code AS "CIG_PRODUCT_CODE",
        sp.cig_product_description AS "CIG_PRODUCT_DESCRIPTION",
        sp.spec_group AS "Spec_Group",
        f.context_project_name AS "Task Plan Project",
        f.context_task_plan_id AS "Task Plan ID",
        f.context_task_plan_creation_date AS "Task Plan Creation Date",
        COALESCE(f.context_task_status, s.life_cycle_state) AS "Task Status",

        'ov_meter_reading' AS "Characteristic",
        TO_CHAR(f.ov_meter_reading) AS "Result",
        TO_CHAR(
            f.ov_meter_reading,
            'FM9999999990.0000',
            'NLS_NUMERIC_CHARACTERS=.,'
        ) AS "Formatted result",
        f.measurement_last_updated AS "Result entered",
        'EQUIPMENT' AS "Result Source",
        'percent' AS "UOM",
        f.context_task_plan_project_plan AS "Task Plan Project Plan"

    FROM equipment_file_values_contexted f
    JOIN hub_owner.sam_sample s
        ON s.id = f.sample_raw_id
    LEFT JOIN hub_owner.sam_sample ms
        ON s.master_sample_id = ms.id
    LEFT JOIN hub_owner.sec_user usr
        ON s.owner_id = usr.id
    LEFT JOIN sample_properties sp
        ON sp.sample_raw_id = s.id
    LEFT JOIN hub_owner.cospc_object_identity coi_sample
        ON coi_sample.object_id = s.id
    LEFT JOIN hub_owner.sec_collab_space cs
        ON cs.id = coi_sample.collaborative_space_id
    WHERE cs.id = '5FD74EE88C024C2EB908BCE0E176B0E8'
      AND NVL(ms.sample_id, 'x') != 'planned'
      AND f.context_project_name IS NOT NULL
      AND f.context_task_plan_project_plan IS NOT NULL
),

all_results AS (
SELECT
    "Sample Name",
    "Sample ID",
    "Sample Status",
    "Master Sample ID",
    "Sampling point",
    "Sampling point description",
    "LINE-1",
    "Owner",
    "Product Code",
    "Product Description",
    "CIG_PRODUCT_CODE",
    "CIG_PRODUCT_DESCRIPTION",
    "Spec_Group",
    "Task Plan Project",
    "Task Plan ID",
    "Task Plan Creation Date",
    "Task Status",
    "Characteristic",
    "Result",
    "Formatted result",
    "Result entered",
    "Result Source",
    "UOM",
    "Task Plan Project Plan"
FROM manual_results

UNION ALL

SELECT
    "Sample Name",
    "Sample ID",
    "Sample Status",
    "Master Sample ID",
    "Sampling point",
    "Sampling point description",
    "LINE-1",
    "Owner",
    "Product Code",
    "Product Description",
    "CIG_PRODUCT_CODE",
    "CIG_PRODUCT_DESCRIPTION",
    "Spec_Group",
    "Task Plan Project",
    "Task Plan ID",
    "Task Plan Creation Date",
    "Task Status",
    "Characteristic",
    "Result",
    "Formatted result",
    "Result entered",
    "Result Source",
    "UOM",
    "Task Plan Project Plan"
FROM equipment_results

UNION ALL

SELECT
    "Sample Name",
    "Sample ID",
    "Sample Status",
    "Master Sample ID",
    "Sampling point",
    "Sampling point description",
    "LINE-1",
    "Owner",
    "Product Code",
    "Product Description",
    "CIG_PRODUCT_CODE",
    "CIG_PRODUCT_DESCRIPTION",
    "Spec_Group",
    "Task Plan Project",
    "Task Plan ID",
    "Task Plan Creation Date",
    "Task Status",
    "Characteristic",
    "Result",
    "Formatted result",
    "Result entered",
    "Result Source",
    "UOM",
    "Task Plan Project Plan"
FROM equipment_file_results

),

ov_meter_readings_ranked AS (
    SELECT
        ar.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                "Sample Name",
                "Sample ID",
                "Sample Status",
                "Master Sample ID",
                "Sampling point",
                "Sampling point description",
                "LINE-1",
                "Owner",
                "Product Code",
                "Product Description",
                "CIG_PRODUCT_CODE",
                "CIG_PRODUCT_DESCRIPTION",
                "Spec_Group",
                "Task Plan Project",
                "Task Plan ID",
                "Task Plan Creation Date",
                "Task Status",
                "Characteristic",
                "Result",
                "Formatted result",
                "Result Source",
                "UOM",
                "Task Plan Project Plan"
            ORDER BY
                "Result entered" DESC NULLS LAST
        ) AS ov_rn
    FROM all_results ar
    WHERE LOWER("Characteristic") = 'ov_meter_reading'
)

SELECT
    "Sample Name",
    "Sample ID",
    "Sample Status",
    "Master Sample ID",
    "Sampling point",
    "Sampling point description",
    "LINE-1",
    "Owner",
    "Product Code",
    "Product Description",
    "CIG_PRODUCT_CODE",
    "CIG_PRODUCT_DESCRIPTION",
    "Spec_Group",
    "Task Plan Project",
    "Task Plan ID",
    "Task Plan Creation Date",
    "Task Status",
    "Characteristic",
    "Result",
    "Formatted result",
    "Result entered",
    "Result Source",
    "UOM",
    "Task Plan Project Plan"
FROM all_results
WHERE LOWER(NVL("Characteristic", 'x')) <> 'ov_meter_reading'

UNION ALL

SELECT
    "Sample Name",
    "Sample ID",
    "Sample Status",
    "Master Sample ID",
    "Sampling point",
    "Sampling point description",
    "LINE-1",
    "Owner",
    "Product Code",
    "Product Description",
    "CIG_PRODUCT_CODE",
    "CIG_PRODUCT_DESCRIPTION",
    "Spec_Group",
    "Task Plan Project",
    "Task Plan ID",
    "Task Plan Creation Date",
    "Task Status",
    "Characteristic",
    "Result",
    "Formatted result",
    "Result entered",
    "Result Source",
    "UOM",
    "Task Plan Project Plan"
FROM ov_meter_readings_ranked
WHERE ov_rn = 1
