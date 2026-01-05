/*
Grain:
(sample, runset, task, measurement, result_item)

Returns only parsed results from $.results[*]
*/

WITH sample_props AS (
  SELECT
    oi.object_id AS sample_raw_id,

    MAX(CASE WHEN p.display_label = 'Sampling Point'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1),
                           TO_CHAR(pv.number_value))
        END) AS sampling_point,

    MAX(CASE WHEN p.display_label = 'Sampling Point Description'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS sampling_point_description,

    MAX(CASE WHEN p.display_label = 'Line'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1),
                           TO_CHAR(pv.number_value))
        END) AS line,

    MAX(CASE WHEN p.display_label = 'Product Code'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1),
                           TO_CHAR(pv.number_value))
        END) AS product_code,

    MAX(CASE WHEN p.display_label = 'Product Description'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS product_description,

    MAX(CASE WHEN p.display_label = 'Cig Product Code'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1),
                           TO_CHAR(pv.number_value))
        END) AS cig_product_code,

    MAX(CASE WHEN p.display_label = 'Cig Product Description'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
        END) AS cig_product_description,

    MAX(CASE WHEN p.display_label = 'Spec group'
             THEN COALESCE(pv.string_value,
                           DBMS_LOB.SUBSTR(pv.long_string_value, 4000, 1))
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
      'Spec group'
    )
  GROUP BY oi.object_id
)

SELECT
  s.name      AS sample_name,
  s.sample_id AS sample_id,

  sp.sampling_point,
  sp.sampling_point_description,
  sp.line,

  u.name AS owner,

  sp.product_code,
  sp.product_description,
  sp.cig_product_code,
  sp.cig_product_description,
  sp.spec_group,

  rp.name            AS task_plan_project,
  t.life_cycle_state AS task_status,
  t.task_id          AS task_id,
  t.task_name        AS task_name,

  m.id               AS measurement_id,

  -- results only
  jt.result_name,
  jt.result_value,
  jt.result_unit_urn

FROM hub_owner.sam_sample s
JOIN hub_owner.req_runset_sample rss
  ON rss.sample_id = s.id
JOIN hub_owner.req_runset rs
  ON rs.id = rss.runset_id
JOIN hub_owner.req_task t
  ON t.runset_id = rs.id

LEFT JOIN hub_owner.res_project rp
  ON rp.id = rs.project_id

LEFT JOIN hub_owner.sec_user u
  ON u.id = s.owner_id

LEFT JOIN sample_props sp
  ON sp.sample_raw_id = s.id

LEFT JOIN hub_owner.res_measurementsample ms
  ON ms.mapped_sample_id = s.id
LEFT JOIN hub_owner.res_measurement m
  ON m.id = ms.measurement_id

CROSS APPLY JSON_TABLE(
  JSON_VALUE(
    NVL(m.raw_data_long_text, TO_CLOB(m.raw_data)),
    '$.results' RETURNING CLOB
  ),
  '$[*]'
  COLUMNS (
    result_name      VARCHAR2(200)   PATH '$.name',
    result_value     VARCHAR2(4000)  PATH '$.value',
    result_unit_urn  VARCHAR2(4000)  PATH '$.unit.urn'
  )
) jt

WHERE jt.result_value IS NOT NULL

ORDER BY
  s.name, s.sample_id, t.task_id, m.id, jt.result_name;
