/* ============================================================
   DIAGNOSTIC: Remaining missing OV rows for S004742-S004760

   Purpose:
   - Check whether these final samples map to instrument sample IDs
   - Check whether linked FILE_CONTENT exists
   - Check whether the instrument sample ID appears in the CSV text
   - Show the matched CSV line and parsed OV value
   ============================================================ */

WITH target_samples AS (
    SELECT 'S004742' AS sample_id FROM dual UNION ALL
    SELECT 'S004745' FROM dual UNION ALL
    SELECT 'S004748' FROM dual UNION ALL
    SELECT 'S004751' FROM dual UNION ALL
    SELECT 'S004754' FROM dual UNION ALL
    SELECT 'S004757' FROM dual UNION ALL
    SELECT 'S004760' FROM dual
),

packet_scope AS (
    SELECT
        ts.sample_id AS target_sample_id,

        s.id AS final_sample_raw_id,
        s.name AS final_sample_name,
        s.sample_id AS final_sample_id,
        s.life_cycle_state AS final_sample_status,

        rms.id AS measurementsample_id,
        rms.sample_id AS instrument_sample_id,
        rms.mapped_sample_id,
        rms.row_index,
        rms.context_id,

        rm.id AS measurement_id,
        rm.record_name,
        rm.measurement_type,
        rm.record_date,
        rm.date_created AS measurement_date_created,
        rm.last_updated AS measurement_last_updated,
        rm.equipment_id,
        rm.equipment_work_item_id,

        e.nickname AS equipment_nickname,
        et.name AS equipment_type_name,
        dp.name AS data_packet_name

    FROM target_samples ts
    JOIN hub_owner.sam_sample s
        ON s.sample_id = ts.sample_id
    JOIN hub_owner.res_measurementsample rms
        ON rms.mapped_sample_id = s.id
    JOIN hub_owner.res_measurement rm
        ON rm.id = rms.measurement_id
    LEFT JOIN hub_owner.res_equipment e
        ON e.id = rm.equipment_id
    LEFT JOIN hub_owner.res_equipment_type et
        ON et.id = e.equipment_type_id
    LEFT JOIN hub_owner.res_data_packet dp
        ON dp.id = et.data_packet_id
    WHERE rm.measurement_type = 'Data Packet'
      AND rm.record_name IS NOT NULL
),

packet_fields AS (
    SELECT
        ps.measurement_id,
        LISTAGG(df.name, ' | ') WITHIN GROUP (ORDER BY df.display_order) AS packet_fields
    FROM packet_scope ps
    JOIN hub_owner.res_equipment e
        ON e.id = ps.equipment_id
    JOIN hub_owner.res_equipment_type et
        ON et.id = e.equipment_type_id
    JOIN hub_owner.res_data_packet dp
        ON dp.id = et.data_packet_id
    JOIN hub_owner.res_data_field df
        ON df.data_packet_id = dp.id
    GROUP BY ps.measurement_id
),

file_candidates AS (
    SELECT
        ps.*,
        pf.packet_fields,

        fm.id AS file_metadata_id,
        fm.name AS file_name,
        fm.storage_type,
        fm.mime_type,
        fm.file_size,
        fm.date_created AS file_date_created,
        fm.last_updated AS file_last_updated,

        DBMS_LOB.GETLENGTH(fc.content) AS blob_length,

        UTL_RAW.CAST_TO_VARCHAR2(
            DBMS_LOB.SUBSTR(fc.content, 12000, 1)
        ) AS file_text

    FROM packet_scope ps
    LEFT JOIN packet_fields pf
        ON pf.measurement_id = ps.measurement_id
    LEFT JOIN hub_owner.file_metadata fm
        ON fm.name = ps.record_name
       AND fm.storage_type = 'DATABASE'
    LEFT JOIN hub_owner.file_content fc
        ON fc.file_id = fm.id
),

matched AS (
    SELECT
        fc.*,

        CASE
            WHEN fc.file_text IS NULL THEN 'NO_FILE_TEXT'
            WHEN INSTR(fc.file_text, fc.instrument_sample_id) > 0 THEN 'INSTRUMENT_SAMPLE_FOUND_IN_FILE'
            ELSE 'INSTRUMENT_SAMPLE_NOT_FOUND_IN_FILE'
        END AS instrument_sample_file_check,

        REGEXP_SUBSTR(
            fc.file_text,
            '(^|' || CHR(10) || ')[^' || CHR(10) || CHR(13) || ']*' ||
                fc.instrument_sample_id ||
            '[^' || CHR(10) || CHR(13) || ']*',
            1,
            1,
            'm'
        ) AS loose_matched_line,

        REGEXP_SUBSTR(
            fc.file_text,
            '(^|' || CHR(10) || ')[^' || CHR(10) || CHR(13) || ']*,' ||
                fc.instrument_sample_id ||
            ',[^' || CHR(10) || CHR(13) || ']*',
            1,
            1,
            'm'
        ) AS strict_matched_line,

        SUBSTR(fc.file_text, 1, 2000) AS file_text_head

    FROM file_candidates fc
),

parsed AS (
    SELECT
        m.*,

        REPLACE(REPLACE(
            COALESCE(m.strict_matched_line, m.loose_matched_line),
            CHR(10),
            ''
        ), CHR(13), '') AS cleaned_line

    FROM matched m
)

SELECT
    target_sample_id,
    final_sample_name,
    final_sample_id,
    final_sample_status,

    instrument_sample_id,
    row_index,
    mapped_sample_id,

    measurement_id,
    record_name,
    measurement_type,
    measurement_date_created,
    measurement_last_updated,

    equipment_nickname,
    equipment_type_name,
    data_packet_name,
    packet_fields,

    file_metadata_id,
    file_name,
    storage_type,
    mime_type,
    file_size,
    blob_length,
    file_date_created,
    file_last_updated,

    instrument_sample_file_check,

    CASE
        WHEN strict_matched_line IS NOT NULL THEN 'STRICT_MATCH'
        WHEN loose_matched_line IS NOT NULL THEN 'LOOSE_MATCH_ONLY'
        ELSE 'NO_MATCH'
    END AS match_type,

    cleaned_line,

    TRIM(REGEXP_SUBSTR(cleaned_line, '(^|,)([^,]*)', 1, 1, NULL, 2)) AS col_1,
    TRIM(REGEXP_SUBSTR(cleaned_line, '(^|,)([^,]*)', 1, 2, NULL, 2)) AS col_2,
    TRIM(REGEXP_SUBSTR(cleaned_line, '(^|,)([^,]*)', 1, 3, NULL, 2)) AS col_3,
    TRIM(REGEXP_SUBSTR(cleaned_line, '(^|,)([^,]*)', 1, 4, NULL, 2)) AS col_4,
    TRIM(REGEXP_SUBSTR(cleaned_line, '(^|,)([^,]*)', 1, 5, NULL, 2)) AS col_5,
    TRIM(REGEXP_SUBSTR(cleaned_line, '(^|,)([^,]*)', 1, 6, NULL, 2)) AS col_6,
    TRIM(REGEXP_SUBSTR(cleaned_line, '(^|,)([^,]*)', 1, 7, NULL, 2)) AS col_7,

    file_text_head

FROM parsed
ORDER BY
    target_sample_id,
    file_last_updated,
    file_size;

    /* ============================================================
   DIAGNOSTIC: Find FILE_METADATA row for remaining missing file

   The failed join was:
       FILE_METADATA.NAME = RES_MEASUREMENT.RECORD_NAME

   Suspect:
       record_name contains slash: 246_Total Blend RL/RCB_03-26-2026-GODWINWS.csv

   This checks:
   - exact record_name
   - basename after slash
   - date/operator token
   - escaped/sanitized slash variants
   ============================================================ */

WITH target_record AS (
    SELECT
        '246_Total Blend RL/RCB_03-26-2026-GODWINWS.csv' AS record_name,
        'RCB_03-26-2026-GODWINWS.csv' AS basename_after_slash,
        '03-26-2026-GODWINWS' AS date_operator_token,
        '246_Total Blend RL' AS prefix_before_slash,
        'RCB' AS token_after_slash
    FROM dual
)
SELECT
    CASE
        WHEN fm.name = tr.record_name THEN 'EXACT_RECORD_NAME'
        WHEN fm.name = tr.basename_after_slash THEN 'BASENAME_AFTER_SLASH'
        WHEN LOWER(fm.name) LIKE '%' || LOWER(tr.date_operator_token) || '%' THEN 'DATE_OPERATOR_TOKEN'
        WHEN LOWER(fm.name) LIKE '%' || LOWER(tr.prefix_before_slash) || '%' THEN 'PREFIX_BEFORE_SLASH'
        WHEN LOWER(fm.name) LIKE '%' || LOWER(tr.token_after_slash) || '%' THEN 'TOKEN_AFTER_SLASH'
        ELSE 'OTHER'
    END AS match_reason,

    RAWTOHEX(fm.id) AS file_metadata_id,
    fm.name AS file_name,
    fm.storage_type,
    fm.mime_type,
    fm.file_size,
    fm.date_created,
    fm.last_updated,

    DBMS_LOB.GETLENGTH(fc.content) AS blob_length,

    CASE
        WHEN fc.content IS NOT NULL
        THEN UTL_RAW.CAST_TO_VARCHAR2(DBMS_LOB.SUBSTR(fc.content, 1000, 1))
    END AS file_text_head

FROM target_record tr
JOIN hub_owner.file_metadata fm
    ON (
           fm.name = tr.record_name
        OR fm.name = tr.basename_after_slash
        OR LOWER(fm.name) LIKE '%' || LOWER(tr.date_operator_token) || '%'
        OR LOWER(fm.name) LIKE '%' || LOWER(REPLACE(tr.record_name, '/', '%')) || '%'
        OR LOWER(fm.name) LIKE '%' || LOWER(tr.prefix_before_slash) || '%'
        OR LOWER(fm.name) LIKE '%' || LOWER(tr.token_after_slash) || '%'
    )
LEFT JOIN hub_owner.file_content fc
    ON fc.file_id = fm.id
ORDER BY
    CASE
        WHEN fm.name = tr.record_name THEN 1
        WHEN fm.name = tr.basename_after_slash THEN 2
        WHEN LOWER(fm.name) LIKE '%' || LOWER(tr.date_operator_token) || '%' THEN 3
        ELSE 9
    END,
    fm.date_created DESC;