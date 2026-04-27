CREATE OR REPLACE PROCEDURE refresh_onelab_result_report AS
    v_last_refresh      TIMESTAMP;
    v_this_refresh      TIMESTAMP := CAST(SYSTIMESTAMP AS TIMESTAMP);
    v_changed_count     NUMBER;
    v_lookback_minutes  NUMBER := 10;
BEGIN
    SELECT last_refresh_ts
    INTO v_last_refresh
    FROM onelab_report_refresh_state
    WHERE id = 1
    FOR UPDATE;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE onelab_changed_task_plans';

    INSERT INTO onelab_changed_task_plans (task_plan_id)
    SELECT DISTINCT runset_id
    FROM (
        /* runset changed */
        SELECT rs.runset_id
        FROM hub_owner.req_runset rs
        WHERE rs.last_updated >= v_last_refresh - NUMTODSINTERVAL(v_lookback_minutes, 'MINUTE')

        UNION

        /* task status / sample list / work item changed */
        SELECT rs.runset_id
        FROM hub_owner.req_task rt
        JOIN hub_owner.req_runset rs
          ON rs.id = rt.runset_id
        WHERE rt.last_updated >= v_last_refresh - NUMTODSINTERVAL(v_lookback_minutes, 'MINUTE')

        UNION

        /* sample changed */
        SELECT rs.runset_id
        FROM hub_owner.req_task rt
        JOIN hub_owner.req_runset rs
          ON rs.id = rt.runset_id
        JOIN hub_owner.sam_sample s
          ON INSTR(',' || rt.sample_list || ',', ',' || s.sample_id || ',') > 0
        WHERE s.last_updated >= v_last_refresh - NUMTODSINTERVAL(v_lookback_minutes, 'MINUTE')

        UNION

        /* manual result changed */
        SELECT rs.runset_id
        FROM hub_owner.cor_parameter_value pv
        JOIN hub_owner.cor_parameter p
          ON pv.parent_identity = p.id
        JOIN hub_owner.req_task_parameter rtp
          ON p.id = rtp.parameter_id
        JOIN hub_owner.req_task rt
          ON rtp.task_id = rt.id
        JOIN hub_owner.req_runset rs
          ON rs.id = rt.runset_id
        WHERE pv.last_updated >= v_last_refresh - NUMTODSINTERVAL(v_lookback_minutes, 'MINUTE')
           OR p.last_updated  >= v_last_refresh - NUMTODSINTERVAL(v_lookback_minutes, 'MINUTE')

        UNION

        /* equipment result/value changed through PEX params */
        SELECT rs.runset_id
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
        JOIN hub_owner.pex_proc_elem_exec pee
          ON pee.parent_id = pe.id
        JOIN hub_owner.pex_proc_elem_exec_param peep
          ON peep.parent_id = pee.id
        JOIN hub_owner.cor_parameter_value pv
          ON pv.parent_identity = peep.id
        WHERE pv.last_updated >= v_last_refresh - NUMTODSINTERVAL(v_lookback_minutes, 'MINUTE')

        UNION

        /* abandoned/completed item-state changes */
        SELECT rs.runset_id
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
        JOIN hub_owner.pex_proc_elem_exec pee
          ON pee.parent_id = pe.id
        WHERE pee.last_updated >= v_last_refresh - NUMTODSINTERVAL(v_lookback_minutes, 'MINUTE')

        UNION

        /* sample extended properties changed */
        SELECT rs.runset_id
        FROM hub_owner.cor_class_identity ci
        JOIN hub_owner.cor_object_identity oi
          ON oi.class_identity_id = ci.id
        JOIN hub_owner.cor_property_value cpv
          ON cpv.object_identity_id = oi.id
        JOIN hub_owner.sam_sample s
          ON s.id = oi.object_id
        JOIN hub_owner.req_task rt
          ON INSTR(',' || rt.sample_list || ',', ',' || s.sample_id || ',') > 0
        JOIN hub_owner.req_runset rs
          ON rs.id = rt.runset_id
        WHERE ci.table_name = 'sam_sample'
          AND cpv.last_updated >= v_last_refresh - NUMTODSINTERVAL(v_lookback_minutes, 'MINUTE')

        UNION

        /* runset extended property changed, e.g. Project Plan */
        SELECT rs.runset_id
        FROM hub_owner.cor_class_identity ci
        JOIN hub_owner.cor_object_identity oi
          ON oi.class_identity_id = ci.id
        JOIN hub_owner.cor_property_value cpv
          ON cpv.object_identity_id = oi.id
        JOIN hub_owner.req_runset rs
          ON rs.id = oi.object_id
        WHERE ci.table_name = 'req_runset'
          AND cpv.last_updated >= v_last_refresh - NUMTODSINTERVAL(v_lookback_minutes, 'MINUTE')

        UNION

        /* fallback sample mapping changed */
        SELECT rs.runset_id
        FROM hub_owner.res_measurementsample rms
        JOIN hub_owner.res_retrieval_context ctx
          ON ctx.id = rms.context_id
        JOIN hub_owner.pex_proc_elem_exec pee
          ON ctx.context =
             'urn:pexelement:' || LOWER(
               SUBSTR(RAWTOHEX(pee.id),1,8)||'-'||
               SUBSTR(RAWTOHEX(pee.id),9,4)||'-'||
               SUBSTR(RAWTOHEX(pee.id),13,4)||'-'||
               SUBSTR(RAWTOHEX(pee.id),17,4)||'-'||
               SUBSTR(RAWTOHEX(pee.id),21,12)
             )
        JOIN hub_owner.pex_proc_exec pe
          ON pe.id = pee.parent_id
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
        WHERE rms.last_updated >= v_last_refresh - NUMTODSINTERVAL(v_lookback_minutes, 'MINUTE')
    );

    SELECT COUNT(*)
    INTO v_changed_count
    FROM onelab_changed_task_plans;

    IF v_changed_count = 0 THEN
        UPDATE onelab_report_refresh_state
        SET last_refresh_ts = v_this_refresh
        WHERE id = 1;

        COMMIT;
        RETURN;
    END IF;

    DELETE FROM onelab_result_report r
    WHERE EXISTS (
        SELECT 1
        FROM onelab_changed_task_plans c
        WHERE c.task_plan_id = r."Task Plan ID"
    );

    INSERT INTO onelab_result_report
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
    FROM (
        SELECT s.*,
               ROW_NUMBER() OVER (
                   PARTITION BY
                       "Task Plan ID",
                       "Sample ID",
                       "Characteristic",
                       "Result Source"
                   ORDER BY "Result entered" DESC NULLS LAST
               ) rn
        FROM vw_onelab_result_source s
        WHERE EXISTS (
            SELECT 1
            FROM onelab_changed_task_plans c
            WHERE c.task_plan_id = s."Task Plan ID"
        )
    )
    WHERE rn = 1;

    UPDATE onelab_report_refresh_state
    SET last_refresh_ts = v_this_refresh
    WHERE id = 1;

    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
/