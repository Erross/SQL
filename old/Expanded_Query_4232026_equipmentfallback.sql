WITH sample_properties AS (
  SELECT
    oi.object_id AS sample_raw_id,
    MAX(CASE WHEN p.display_label = 'Sampling Point'
             THEN COALESCE(pv.string_value,
                           SUBSTR(TO_CHAR(pv.long_string_value), 1, 4000),
                           TO_CHAR(pv.number_value))
        END) AS sampling_point,
    MAX(CASE WHEN p.display_label = 'Sampling Point Description'
             THEN COALESCE(pv.string_value,
                           SUBSTR(TO_CHAR(pv.long_string_value), 1, 4000))
        END) AS sampling_point_description,
    MAX(CASE WHEN p.display_label = 'Line'
             THEN COALESCE(pv.string_value,
                           SUBSTR(TO_CHAR(pv.long_string_value), 1, 4000),
                           TO_CHAR(pv.number_value))
        END) AS line,
    MAX(CASE WHEN p.display_label = 'Product Code'
             THEN COALESCE(pv.string_value,
                           SUBSTR(TO_CHAR(pv.long_string_value), 1, 4000),
                           TO_CHAR(pv.number_value))
        END) AS product_code,
    MAX(CASE WHEN p.display_label = 'Product Description'
             THEN COALESCE(pv.string_value,
                           SUBSTR(TO_CHAR(pv.long_string_value), 1, 4000))
        END) AS product_description,
    MAX(CASE WHEN p.display_label = 'Cig Product Code'
             THEN COALESCE(pv.string_value,
                           SUBSTR(TO_CHAR(pv.long_string_value), 1, 4000),
                           TO_CHAR(pv.number_value))
        END) AS cig_product_code,
    MAX(CASE WHEN p.display_label = 'Cig Product Description'
             THEN COALESCE(pv.string_value,
                           SUBSTR(TO_CHAR(pv.long_string_value), 1, 4000))
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
    ve.name      AS tp_project_plan
  FROM hub_owner.cor_class_identity ci
  JOIN hub_owner.cor_object_identity oi
    ON oi.class_identity_id = ci.id
  JOIN hub_owner.cor_property_value pv
    ON pv.object_identity_id = oi.id
  JOIN hub_owner.cor_property p
    ON p.name = pv.property_id
  JOIN hub_owner.cor_vocab_entry ve
    ON RAWTOHEX(ve.id) = UPPER(REPLACE(pv.string_value, '-', ''))
  WHERE ci.table_name   = 'req_runset'
    AND p.display_label = 'Project Plan'
    AND pv.string_value IS NOT NULL
),

/* ============================================================
   EQUIPMENT PARAMETER METADATA
   ============================================================ */
equipment_param_defs AS (
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

/* ============================================================
   DYNAMIC RESULT CANDIDATES
   Metadata fields are excluded; remaining valued fields are
   treated as possible result fields.
   ============================================================ */
equipment_classified AS (
  SELECT
    en.*,
    CASE
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

/* ============================================================
   ONE ROW PER EQUIPMENT RESULT
   Packet metadata is extracted explicitly.
   Result is chosen dynamically from non-metadata fields.
   ============================================================ */
equipment_rows AS (
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
        END) AS meter_number,

    /* dynamic result selection */
    MAX(CASE
          WHEN field_role = 'CANDIDATE_RESULT'
           AND value_numeric IS NOT NULL
          THEN value_numeric
        END) AS result_numeric,

    MAX(CASE
          WHEN field_role = 'CANDIDATE_RESULT'
           AND (
                value_string IS NOT NULL OR
                value_text IS NOT NULL OR
                value_numeric_text IS NOT NULL OR
                value_numeric IS NOT NULL
               )
          THEN COALESCE(value_string, value_text, value_numeric_text, TO_CHAR(value_numeric))
        END) AS result_text,

    MAX(CASE
          WHEN field_role = 'CANDIDATE_RESULT'
           AND (
                value_numeric IS NOT NULL OR
                value_string IS NOT NULL OR
                value_text IS NOT NULL OR
                value_numeric_text IS NOT NULL
               )
          THEN interpretation
        END) AS result_interpretation,

    MAX(CASE
          WHEN field_role = 'CANDIDATE_RESULT'
           AND (
                value_numeric IS NOT NULL OR
                value_string IS NOT NULL OR
                value_text IS NOT NULL OR
                value_numeric_text IS NOT NULL
               )
          THEN last_updated
        END) AS result_entered,

    MAX(CASE
          WHEN field_role = 'CANDIDATE_RESULT'
           AND (
                value_numeric IS NOT NULL OR
                value_string IS NOT NULL OR
                value_text IS NOT NULL OR
                value_numeric_text IS NOT NULL
               )
          THEN unit_id
        END) AS result_unit_id,

    MAX(CASE
          WHEN field_role = 'CANDIDATE_RESULT'
           AND (
                value_numeric IS NOT NULL OR
                value_string IS NOT NULL OR
                value_text IS NOT NULL OR
                value_numeric_text IS NOT NULL
               )
          THEN field_name
        END) AS result_field_name

  FROM equipment_classified
  GROUP BY proc_exec_id, proc_elem_exec_id, item_index, group_index
),

/* ============================================================
   ADD RETRIEVAL CONTEXT TO EQUIPMENT ROWS
   ============================================================ */
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

/* ============================================================
   FALLBACK MEASUREMENT ORDINALS
   ============================================================ */
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

/* ============================================================
   RESOLVE SAMPLE:
   1) explicit packet sample_id if present
   2) otherwise fallback by context ordinal
   ============================================================ */
equipment_resolved AS (
  SELECT
    ewc.proc_exec_id,
    ewc.proc_elem_exec_id,
    ewc.item_index,
    ewc.group_index,
    ewc.packet_sample_id,
    ewc.data_id,
    ewc.meter_number,
    ewc.result_numeric,
    ewc.result_text,
    ewc.result_interpretation,
    ewc.result_entered,
    ewc.result_unit_id,
    ewc.result_field_name,
    ewc.context_id,

    s_packet.id AS packet_sample_raw_id,
    s_packet.sample_id AS packet_sample_id_resolved,

    s_fallback.id AS fallback_sample_raw_id,
    s_fallback.sample_id AS fallback_sample_id_resolved,

    COALESCE(s_packet.id, s_fallback.id) AS resolved_sample_raw_id,
    COALESCE(s_packet.sample_id, s_fallback.sample_id) AS resolved_sample_id
  FROM equipment_with_context ewc
  LEFT JOIN hub_owner.sam_sample s_packet
    ON s_packet.sample_id = ewc.packet_sample_id
  LEFT JOIN fallback_measurements fm
    ON fm.context_id = ewc.context_id
   AND fm.derived_item_index = ewc.item_index
   AND ewc.packet_sample_id IS NULL
  LEFT JOIN hub_owner.sam_sample s_fallback
    ON s_fallback.id = fm.mapped_sample_id
),

/* ============================================================
   MANUAL SIDE
   ============================================================ */
manual_results AS (
  SELECT DISTINCT
    s.NAME                      AS "Sample Name",
    s.SAMPLE_ID                 AS "Sample ID",
    (SELECT CASE
              WHEN MAX(CASE WHEN
                   UPPER(SUBSTR(pee2.ITEM_STATES,
                       (SELECT COUNT(*) + 1
                        FROM hub_owner.REQ_TASK rt2
                        JOIN hub_owner.SAM_SAMPLE s2
                          ON INSTR(','||rt2.SAMPLE_LIST||',', ','||s2.SAMPLE_ID||',') > 0
                        WHERE rt2.WORK_ITEM LIKE '%' || LOWER(
                                SUBSTR(RAWTOHEX(pe2.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe2.ID),9,4)||'-'||
                                SUBSTR(RAWTOHEX(pe2.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe2.ID),17,4)||'-'||
                                SUBSTR(RAWTOHEX(pe2.ID),21,12)) || '%'
                          AND s2.SAMPLE_ID < s.SAMPLE_ID),
                       1)) = 'X' THEN 1 END) = 1 THEN 'abandoned'
              WHEN MAX(CASE WHEN
                   UPPER(SUBSTR(pee2.ITEM_STATES,
                       (SELECT COUNT(*) + 1
                        FROM hub_owner.REQ_TASK rt2
                        JOIN hub_owner.SAM_SAMPLE s2
                          ON INSTR(','||rt2.SAMPLE_LIST||',', ','||s2.SAMPLE_ID||',') > 0
                        WHERE rt2.WORK_ITEM LIKE '%' || LOWER(
                                SUBSTR(RAWTOHEX(pe2.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe2.ID),9,4)||'-'||
                                SUBSTR(RAWTOHEX(pe2.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe2.ID),17,4)||'-'||
                                SUBSTR(RAWTOHEX(pe2.ID),21,12)) || '%'
                          AND s2.SAMPLE_ID < s.SAMPLE_ID),
                       1)) = 'D' THEN 1 END) = 1 THEN 'completed'
              ELSE s.LIFE_CYCLE_STATE
            END
     FROM hub_owner.PEX_PROC_EXEC pe2
     JOIN hub_owner.PEX_PROC_ELEM_EXEC pee2
       ON pee2.PARENT_ID = pe2.ID
     WHERE rt.WORK_ITEM LIKE '%' || LOWER(
              SUBSTR(RAWTOHEX(pe2.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe2.ID),9,4)||'-'||
              SUBSTR(RAWTOHEX(pe2.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe2.ID),17,4)||'-'||
              SUBSTR(RAWTOHEX(pe2.ID),21,12)) || '%'
       AND pee2.ITEM_STATES IS NOT NULL
    )                           AS "Sample Status",
    ms.SAMPLE_ID                AS "Master Sample ID",
    sp.sampling_point           AS "Sampling point",
    TRIM(REGEXP_REPLACE(
      REPLACE(REPLACE(sp.sampling_point_description, CHR(13), ''), CHR(10), ''),
      '\s*\[[[:digit:]]+\]\s*$',''
    ))                          AS "Sampling point description",
    sp.line                     AS "LINE-1",
    usr.NAME                    AS "Owner",
    sp.product_code             AS "Product Code",
    sp.product_description      AS "Product Description",
    sp.cig_product_code         AS "CIG_PRODUCT_CODE",
    sp.cig_product_description  AS "CIG_PRODUCT_DESCRIPTION",
    sp.spec_group               AS "Spec_Group",
    proj.NAME                   AS "Task Plan Project",
    runset.RUNSET_ID            AS "Task Plan ID",
    runset.DATE_CREATED         AS "Task Plan Creation Date",
    rt.LIFE_CYCLE_STATE         AS "Task Status",
    p.DISPLAY_NAME              AS "Characteristic",
    pv.INTERPRETATION           AS "Compose Details",
    pv.VALUE_STRING             AS "Result",
    pv.VALUE_TEXT               AS "Formatted result",
    pv.LAST_UPDATED             AS "Result entered",
    cs.NAME                     AS "Collaboration Space",
    'MANUAL'                    AS "Result Source",
    uom.description             AS "UOM",
    (SELECT MAX(pee2.SOURCE_POSITION||':'||pee2.ITEM_STATES)
     FROM hub_owner.PEX_PROC_EXEC pe2
     JOIN hub_owner.PEX_PROC_ELEM_EXEC pee2
       ON pee2.PARENT_ID = pe2.ID
     WHERE rt.WORK_ITEM LIKE '%' || LOWER(
              SUBSTR(RAWTOHEX(pe2.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe2.ID),9,4)||'-'||
              SUBSTR(RAWTOHEX(pe2.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe2.ID),17,4)||'-'||
              SUBSTR(RAWTOHEX(pe2.ID),21,12)) || '%'
       AND pee2.ITEM_STATES IS NOT NULL
       AND REGEXP_LIKE(pee2.ITEM_STATES, '[XD]')
    )                           AS "Item States (Debug)",
    rp.tp_project_plan          AS "Task Plan Project Plan"
  FROM hub_owner.COR_PARAMETER_VALUE pv
  JOIN hub_owner.COR_PARAMETER p
    ON pv.PARENT_IDENTITY = p.ID
  JOIN hub_owner.REQ_TASK_PARAMETER rtp
    ON p.ID = rtp.PARAMETER_ID
  JOIN hub_owner.REQ_TASK rt
    ON rtp.TASK_ID = rt.ID
  LEFT JOIN hub_owner.REQ_RUNSET runset
    ON rt.RUNSET_ID = runset.ID
  LEFT JOIN hub_owner.SAM_SAMPLE s
    ON s.SAMPLE_ID = REGEXP_SUBSTR(rt.SAMPLE_LIST, '[^,]+', 1, pv.ITEM_INDEX + 1)
  LEFT JOIN hub_owner.SAM_SAMPLE ms
    ON s.MASTER_SAMPLE_ID = ms.ID
  LEFT JOIN hub_owner.SEC_USER usr
    ON s.OWNER_ID = usr.ID
  LEFT JOIN hub_owner.RES_PROJECT proj
    ON runset.PROJECT_ID = proj.ID
  LEFT JOIN sample_properties sp
    ON sp.sample_raw_id = s.ID
  LEFT JOIN runset_properties rp
    ON rp.runset_raw_id = runset.ID
  LEFT JOIN hub_owner.COSPC_OBJECT_IDENTITY coi_sample
    ON coi_sample.OBJECT_ID = s.ID
  LEFT JOIN hub_owner.SEC_COLLAB_SPACE cs
    ON cs.ID = coi_sample.COLLABORATIVE_SPACE_ID
  LEFT JOIN hub_owner.COR_UNIT uom
    ON pv.UNIT = uom.ID
  WHERE pv.VALUE_KEY = 'A'
    AND s.SAMPLE_ID IS NOT NULL
    AND ms.SAMPLE_ID != 'planned'
    AND p.DISPLAY_NAME != 'Sample'
    AND rt.LIFE_CYCLE_STATE IN ('released', 'completed')
    AND p.VALUE_TYPE NOT IN ('Vocabulary')
    AND pv.VALUE_STRING IS NOT NULL
    AND cs.ID = '5FD74EE88C024C2EB908BCE0E176B0E8'
),

/* ============================================================
   EQUIPMENT SIDE
   ============================================================ */
equipment_results AS (
  SELECT DISTINCT
    s.NAME                      AS "Sample Name",
    s.SAMPLE_ID                 AS "Sample ID",
    (SELECT CASE
              WHEN MAX(CASE WHEN
                   UPPER(SUBSTR(pee2.ITEM_STATES,
                       (SELECT COUNT(*) + 1
                        FROM hub_owner.REQ_TASK rt2
                        JOIN hub_owner.SAM_SAMPLE s2
                          ON INSTR(','||rt2.SAMPLE_LIST||',', ','||s2.SAMPLE_ID||',') > 0
                        WHERE rt2.WORK_ITEM LIKE '%' || LOWER(
                                SUBSTR(RAWTOHEX(pe2.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe2.ID),9,4)||'-'||
                                SUBSTR(RAWTOHEX(pe2.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe2.ID),17,4)||'-'||
                                SUBSTR(RAWTOHEX(pe2.ID),21,12)) || '%'
                          AND s2.SAMPLE_ID < s.SAMPLE_ID),
                       1)) = 'X' THEN 1 END) = 1 THEN 'abandoned'
              WHEN MAX(CASE WHEN
                   UPPER(SUBSTR(pee2.ITEM_STATES,
                       (SELECT COUNT(*) + 1
                        FROM hub_owner.REQ_TASK rt2
                        JOIN hub_owner.SAM_SAMPLE s2
                          ON INSTR(','||rt2.SAMPLE_LIST||',', ','||s2.SAMPLE_ID||',') > 0
                        WHERE rt2.WORK_ITEM LIKE '%' || LOWER(
                                SUBSTR(RAWTOHEX(pe2.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe2.ID),9,4)||'-'||
                                SUBSTR(RAWTOHEX(pe2.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe2.ID),17,4)||'-'||
                                SUBSTR(RAWTOHEX(pe2.ID),21,12)) || '%'
                          AND s2.SAMPLE_ID < s.SAMPLE_ID),
                       1)) = 'D' THEN 1 END) = 1 THEN 'completed'
              ELSE s.LIFE_CYCLE_STATE
            END
     FROM hub_owner.PEX_PROC_EXEC pe2
     JOIN hub_owner.PEX_PROC_ELEM_EXEC pee2
       ON pee2.PARENT_ID = pe2.ID
     WHERE rt.WORK_ITEM LIKE '%' || LOWER(
              SUBSTR(RAWTOHEX(pe2.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe2.ID),9,4)||'-'||
              SUBSTR(RAWTOHEX(pe2.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe2.ID),17,4)||'-'||
              SUBSTR(RAWTOHEX(pe2.ID),21,12)) || '%'
       AND pee2.ITEM_STATES IS NOT NULL
    )                           AS "Sample Status",
    ms.SAMPLE_ID                AS "Master Sample ID",
    sp.sampling_point           AS "Sampling point",
    TRIM(REGEXP_REPLACE(
      REPLACE(REPLACE(sp.sampling_point_description, CHR(13), ''), CHR(10), ''),
      '\s*\[[[:digit:]]+\]\s*$',''
    ))                          AS "Sampling point description",
    sp.line                     AS "LINE-1",
    usr.NAME                    AS "Owner",
    sp.product_code             AS "Product Code",
    sp.product_description      AS "Product Description",
    sp.cig_product_code         AS "CIG_PRODUCT_CODE",
    sp.cig_product_description  AS "CIG_PRODUCT_DESCRIPTION",
    sp.spec_group               AS "Spec_Group",
    proj.NAME                   AS "Task Plan Project",
    runset.RUNSET_ID            AS "Task Plan ID",
    runset.DATE_CREATED         AS "Task Plan Creation Date",
    rt.LIFE_CYCLE_STATE         AS "Task Status",
    NVL(er.result_field_name, 'Equipment Result') AS "Characteristic",
    er.result_interpretation    AS "Compose Details",
    TO_CHAR(er.result_numeric)  AS "Result",
    NVL(er.result_text, TO_CHAR(er.result_numeric)) AS "Formatted result",
    er.result_entered           AS "Result entered",
    cs.NAME                     AS "Collaboration Space",
    'EQUIPMENT'                 AS "Result Source",
    uom.description             AS "UOM",
    (SELECT MAX(pee2.SOURCE_POSITION||':'||pee2.ITEM_STATES)
     FROM hub_owner.PEX_PROC_EXEC pe2
     JOIN hub_owner.PEX_PROC_ELEM_EXEC pee2
       ON pee2.PARENT_ID = pe2.ID
     WHERE rt.WORK_ITEM LIKE '%' || LOWER(
              SUBSTR(RAWTOHEX(pe2.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe2.ID),9,4)||'-'||
              SUBSTR(RAWTOHEX(pe2.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe2.ID),17,4)||'-'||
              SUBSTR(RAWTOHEX(pe2.ID),21,12)) || '%'
       AND pee2.ITEM_STATES IS NOT NULL
       AND REGEXP_LIKE(pee2.ITEM_STATES, '[XD]')
    )                           AS "Item States (Debug)",
    rp.tp_project_plan          AS "Task Plan Project Plan"
  FROM equipment_resolved er
  JOIN hub_owner.PEX_PROC_EXEC pe
    ON pe.ID = er.proc_exec_id
  JOIN hub_owner.REQ_TASK rt
    ON rt.WORK_ITEM LIKE '%' || LOWER(
         SUBSTR(RAWTOHEX(pe.ID),1,8)||'-'||SUBSTR(RAWTOHEX(pe.ID),9,4)||'-'||
         SUBSTR(RAWTOHEX(pe.ID),13,4)||'-'||SUBSTR(RAWTOHEX(pe.ID),17,4)||'-'||
         SUBSTR(RAWTOHEX(pe.ID),21,12)
       ) || '%'
  JOIN hub_owner.SAM_SAMPLE s
    ON s.ID = er.resolved_sample_raw_id
   AND INSTR(','||rt.SAMPLE_LIST||',', ','||s.SAMPLE_ID||',') > 0
  LEFT JOIN hub_owner.SAM_SAMPLE ms
    ON s.MASTER_SAMPLE_ID = ms.ID
  LEFT JOIN hub_owner.SEC_USER usr
    ON s.OWNER_ID = usr.ID
  LEFT JOIN sample_properties sp
    ON sp.sample_raw_id = s.ID
  LEFT JOIN hub_owner.REQ_RUNSET runset
    ON rt.RUNSET_ID = runset.ID
  LEFT JOIN hub_owner.RES_PROJECT proj
    ON runset.PROJECT_ID = proj.ID
  LEFT JOIN runset_properties rp
    ON rp.runset_raw_id = runset.ID
  LEFT JOIN hub_owner.COSPC_OBJECT_IDENTITY coi_sample
    ON coi_sample.OBJECT_ID = s.ID
  LEFT JOIN hub_owner.SEC_COLLAB_SPACE cs
    ON cs.ID = coi_sample.COLLABORATIVE_SPACE_ID
  LEFT JOIN hub_owner.COR_UNIT uom
    ON uom.ID = er.result_unit_id
  WHERE er.resolved_sample_raw_id IS NOT NULL
    AND rt.LIFE_CYCLE_STATE IN ('released', 'completed')
    AND cs.ID = '5FD74EE88C024C2EB908BCE0E176B0E8'
    AND ms.SAMPLE_ID != 'planned'
    AND (
         er.result_numeric IS NOT NULL OR
         er.result_text IS NOT NULL
        )
)

SELECT * FROM manual_results
UNION ALL
SELECT * FROM equipment_results;


'S007162','S007165','S007168','S007171','S007174',
'S007178','S007181','S007184','S007187','S007190',
'S005421','S002658','S005460','S003358','S004745',
'S002689','S005556','S003047','S003049','S005447',
'S003044','S005415','S006384','S004769','S005565',
'S005602','S004362','S003374','S003317'