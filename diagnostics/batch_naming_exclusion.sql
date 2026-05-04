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