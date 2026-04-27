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
        MAX(CASE WHEN pv.value_key = 'A' THEN pv.value_numeric_text END) AS value_numeric_text
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
        v.item_index,
        v.group_index,
        d.field_name,
        v.value_string,
        v.value_numeric,
        v.value_text,
        v.value_numeric_text
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
            END) AS result_numeric
    FROM equipment_named
    GROUP BY proc_exec_id, proc_elem_exec_id, item_index, group_index
),
chk AS (
    SELECT
        er.packet_sample_id,
        er.item_index,
        er.result_numeric
    FROM equipment_rows er
    WHERE er.packet_sample_id IN ('S007178','S007181','S007184','S007187','S007190')
)
SELECT CASE
         WHEN COUNT(*) = 5
          AND MAX(CASE WHEN packet_sample_id = 'S007178' AND item_index = 5 AND result_numeric IS NOT NULL THEN 1 ELSE 0 END) = 1
          AND MAX(CASE WHEN packet_sample_id = 'S007181' AND item_index = 6 AND result_numeric IS NOT NULL THEN 1 ELSE 0 END) = 1
          AND MAX(CASE WHEN packet_sample_id = 'S007184' AND item_index = 7 AND result_numeric IS NOT NULL THEN 1 ELSE 0 END) = 1
          AND MAX(CASE WHEN packet_sample_id = 'S007187' AND item_index = 8 AND result_numeric IS NOT NULL THEN 1 ELSE 0 END) = 1
          AND MAX(CASE WHEN packet_sample_id = 'S007190' AND item_index = 9 AND result_numeric IS NOT NULL THEN 1 ELSE 0 END) = 1
           THEN 'THIS WORKED'
         ELSE 'OH NO'
       END AS status
FROM chk;