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
        tm.proc_exec_id,
        s.sample_id,
        COALESCE(
            issf.item_state_status,
            tm.task_status,
            s.life_cycle_state
        ) AS derived_status
    FROM task_map tm
    JOIN hub_owner.sam_sample s
        ON INSTR(',' || tm.sample_list || ',', ',' || s.sample_id || ',') > 0
    LEFT JOIN item_state_sample_flags issf
        ON issf.proc_exec_id = tm.proc_exec_id
       AND issf.sample_id = s.sample_id
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

equipment_result_candidates AS (
    SELECT
        ec.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                ec.proc_exec_id,
                ec.proc_elem_exec_id,
                ec.item_index,
                ec.group_index
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
),

equipment_selected_result AS (
    SELECT *
    FROM equipment_result_candidates
    WHERE rn = 1
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

        COALESCE(
            s_packet.id,
            s_fb_multi.id,
            s_fb_single.id,
            s_fb_task.id
        ) AS sample_raw_id,

        COALESCE(
            s_packet.sample_id,
            s_fb_multi.sample_id,
            s_fb_single.sample_id,
            s_fb_task.sample_id
        ) AS sample_id

    FROM equipment_with_context ewc
    LEFT JOIN equipment_packet_shape eps
        ON eps.proc_elem_exec_id = ewc.proc_elem_exec_id
    LEFT JOIN hub_owner.sam_sample s_packet
        ON s_packet.sample_id = ewc.packet_sample_id
    LEFT JOIN fallback_measurements fm_multi
        ON fm_multi.context_id = ewc.context_id
       AND fm_multi.derived_item_index = ewc.item_index
       AND s_packet.id IS NULL
       AND NVL(eps.max_item_index, 0) > 0
    LEFT JOIN hub_owner.sam_sample s_fb_multi
        ON s_fb_multi.id = fm_multi.mapped_sample_id
    LEFT JOIN fallback_measurements fm_single
        ON fm_single.context_id = ewc.context_id
       AND s_packet.id IS NULL
       AND s_fb_multi.id IS NULL
       AND NVL(eps.max_item_index, 0) = 0
    LEFT JOIN hub_owner.sam_sample s_fb_single
        ON s_fb_single.id = fm_single.mapped_sample_id
    LEFT JOIN task_map tm_resolve
        ON tm_resolve.proc_exec_id = ewc.proc_exec_id
    LEFT JOIN hub_owner.sam_sample s_fb_task
        ON s_fb_task.sample_id = REGEXP_SUBSTR(
            tm_resolve.sample_list,
            '[^,]+',
            1,
            ewc.item_index + 1
        )
       AND s_packet.id IS NULL
       AND s_fb_multi.id IS NULL
       AND s_fb_single.id IS NULL
),

/* ============================================================
   NEW OV METER FILE-CONTENT PATH

   This recovers OV_Meter_Reading from uploaded equipment CSVs.

   Mapping:
       final sample:
           hub_owner.sam_sample.id
       to packet row:
           hub_owner.res_measurementsample.mapped_sample_id
       to instrument CSV sample:
           hub_owner.res_measurementsample.sample_id
       to file:
           hub_owner.res_measurement.record_name = hub_owner.file_metadata.name
       to value:
           FILE_CONTENT CSV Sample_Id = RES_MEASUREMENTSAMPLE.SAMPLE_ID
           FILE_CONTENT CSV OV_Meter_Reading = report Result
   ============================================================ */

equipment_file_packet_scope AS (
    SELECT DISTINCT
        s.id AS sample_raw_id,
        s.name AS sample_name,
        s.sample_id AS sample_id,

        rms.id AS measurementsample_raw_id,
        rms.sample_id AS instrument_sample_id,
        rms.row_index,
        rms.context_id,

        rm.id AS measurement_raw_id,
        rm.record_name,
        rm.record_date,
        rm.date_created AS measurement_created,
        rm.last_updated AS measurement_last_updated,
        rm.equipment_id,
        rm.equipment_work_item_id,

        et.name AS equipment_type_name,
        dp.id AS data_packet_id,
        dp.name AS data_packet_name

    FROM hub_owner.sam_sample s
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
    WHERE rm.measurement_type = 'Data Packet'
      AND rm.record_name IS NOT NULL
      AND rms.sample_id IS NOT NULL
      AND LOWER(rm.record_name) LIKE '%.csv'
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
        ON fm.name = ps.record_name
       AND fm.storage_type = 'DATABASE'
    JOIN hub_owner.file_content fc
        ON fc.file_id = fm.id
    WHERE DBMS_LOB.GETLENGTH(fc.content) <= 4000
),

numbers AS (
    SELECT LEVEL AS n
    FROM dual
    CONNECT BY LEVEL <= 200
),

equipment_file_csv_lines AS (
    SELECT
        eft.*,
        n.n AS line_no,
        TRIM(
            REPLACE(
                REGEXP_SUBSTR(
                    eft.file_text,
                    '[^' || CHR(10) || ']+',
                    1,
                    n.n
                ),
                CHR(13),
                ''
            )
        ) AS line_text
    FROM equipment_file_text eft
    JOIN numbers n
        ON n.n <= REGEXP_COUNT(eft.file_text, CHR(10)) + 1
),

equipment_file_csv_parsed AS (
    SELECT
        cl.*,

        TRIM(REGEXP_SUBSTR(cl.line_text, '(^|,)([^,]*)', 1, 1, NULL, 2)) AS csv_group_name,
        TRIM(REGEXP_SUBSTR(cl.line_text, '(^|,)([^,]*)', 1, 2, NULL, 2)) AS csv_number_in_group,
        TRIM(REGEXP_SUBSTR(cl.line_text, '(^|,)([^,]*)', 1, 3, NULL, 2)) AS csv_data_id,
        TRIM(REGEXP_SUBSTR(cl.line_text, '(^|,)([^,]*)', 1, 4, NULL, 2)) AS csv_sample_id,
        TRIM(REGEXP_SUBSTR(cl.line_text, '(^|,)([^,]*)', 1, 5, NULL, 2)) AS csv_meter_number,
        TRIM(REGEXP_SUBSTR(cl.line_text, '(^|,)([^,]*)', 1, 6, NULL, 2)) AS csv_ov_meter_reading,
        TRIM(REGEXP_SUBSTR(cl.line_text, '(^|,)([^,]*)', 1, 7, NULL, 2)) AS csv_sampling_point_time

    FROM equipment_file_csv_lines cl
    WHERE cl.line_no > 1
),

equipment_file_values AS (
    SELECT
        p.sample_raw_id,
        p.sample_name,
        p.sample_id,
        p.instrument_sample_id,
        p.measurementsample_raw_id,
        p.measurement_raw_id,
        p.record_name,
        p.record_date,
        p.measurement_created,
        p.measurement_last_updated,
        p.file_metadata_id,
        p.file_name,
        p.file_size,
        p.file_date_created,
        p.file_last_updated,
        p.csv_data_id,
        p.csv_meter_number,
        p.csv_sampling_point_time,

        TO_NUMBER(
            p.csv_ov_meter_reading,
            '9999999990D999999',
            'NLS_NUMERIC_CHARACTERS=.,'
        ) AS ov_meter_reading,

        ROW_NUMBER() OVER (
            PARTITION BY p.sample_id
            ORDER BY
                p.measurement_last_updated DESC NULLS LAST,
                p.file_last_updated DESC NULLS LAST,
                p.file_date_created DESC NULLS LAST,
                TO_NUMBER(
                    CASE
                        WHEN REGEXP_LIKE(p.csv_data_id, '^[0-9]+$')
                        THEN p.csv_data_id
                    END
                ) DESC NULLS LAST
        ) AS rn

    FROM equipment_file_csv_parsed p
    WHERE p.csv_group_name = 'Data'
      AND p.csv_sample_id = p.instrument_sample_id
      AND REGEXP_LIKE(p.csv_ov_meter_reading, '^-?[0-9]+(\.[0-9]+)?$')
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
        tm.task_status AS "Task Status",
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
    JOIN task_map tm
        ON tm.task_raw_id = rt.id
    JOIN sample_exec_status ses
        ON ses.sample_id = REGEXP_SUBSTR(rt.sample_list, '[^,]+', 1, pv.item_index + 1)
       AND ses.proc_exec_id = tm.proc_exec_id
    JOIN hub_owner.sam_sample s
        ON s.sample_id = ses.sample_id
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
      AND ms.sample_id != 'planned'
      AND p.display_name != 'Sample'
      AND p.value_type NOT IN ('Vocabulary')
      AND pv.value_string IS NOT NULL
      AND cs.id = '5FD74EE88C024C2EB908BCE0E176B0E8'
),

equipment_results AS (
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
        ON tm.proc_exec_id = er.proc_exec_id
    JOIN sample_exec_status ses
        ON ses.sample_id = er.sample_id
       AND ses.proc_exec_id = tm.proc_exec_id
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
      AND ms.sample_id != 'planned'
      AND er.value_numeric IS NOT NULL
),

existing_report_anchor AS (
    SELECT *
    FROM (
        SELECT
            r.*,
            ROW_NUMBER() OVER (
                PARTITION BY r."Sample ID"
                ORDER BY
                    CASE
                        WHEN LOWER(r."Characteristic") = 'ov meter deviation' THEN 1
                        WHEN LOWER(r."Characteristic") = 'dish difference' THEN 2
                        WHEN LOWER(r."Characteristic") = 'dish average' THEN 3
                        ELSE 9
                    END,
                    r."Result entered" DESC NULLS LAST
            ) AS anchor_rn
        FROM (
            SELECT * FROM manual_results
            UNION ALL
            SELECT * FROM equipment_results
        ) r
        WHERE LOWER(r."Characteristic") IN (
            'ov meter deviation',
            'dish difference',
            'dish average'
        )
    )
    WHERE anchor_rn = 1
),

equipment_file_results AS (
    SELECT
        a."Sample Name" AS "Sample Name",
        a."Sample ID" AS "Sample ID",
        a."Sample Status" AS "Sample Status",
        a."Master Sample ID" AS "Master Sample ID",
        a."Sampling point" AS "Sampling point",
        a."Sampling point description" AS "Sampling point description",
        a."LINE-1" AS "LINE-1",
        a."Owner" AS "Owner",
        a."Product Code" AS "Product Code",
        a."Product Description" AS "Product Description",
        a."CIG_PRODUCT_CODE" AS "CIG_PRODUCT_CODE",
        a."CIG_PRODUCT_DESCRIPTION" AS "CIG_PRODUCT_DESCRIPTION",
        a."Spec_Group" AS "Spec_Group",
        a."Task Plan Project" AS "Task Plan Project",
        a."Task Plan ID" AS "Task Plan ID",
        a."Task Plan Creation Date" AS "Task Plan Creation Date",
        a."Task Status" AS "Task Status",

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

        a."Task Plan Project Plan" AS "Task Plan Project Plan"

    FROM equipment_file_values f
    JOIN existing_report_anchor a
        ON a."Sample ID" = f.sample_id
    WHERE f.rn = 1
      AND NOT EXISTS (
          SELECT 1
          FROM equipment_results er
          WHERE er."Sample ID" = f.sample_id
            AND LOWER(er."Characteristic") = 'ov_meter_reading'
      )
)

SELECT DISTINCT *
FROM (
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
);