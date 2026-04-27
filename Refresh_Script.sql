BEGIN
  DBMS_SCHEDULER.CREATE_JOB (
    job_name        => 'ONELAB_RESULT_REPORT_REFRESH_JOB',
    job_type        => 'PLSQL_BLOCK',
    job_action      => q'[
DECLARE
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
        SELECT rs.runset_id
        FROM hub_owner.req_runset rs
        WHERE rs.last_updated >= v_last_refresh - NUMTODSINTERVAL(v_lookback_minutes, 'MINUTE')

        UNION

        SELECT rs.runset_id
        FROM hub_owner.req_task rt
        JOIN hub_owner.req_runset rs
          ON rs.id = rt.runset_id
        WHERE rt.last_updated >= v_last_refresh - NUMTODSINTERVAL(v_lookback_minutes, 'MINUTE')

        UNION

        SELECT rs.runset_id
        FROM hub_owner.req_task rt
        JOIN hub_owner.req_runset rs
          ON rs.id = rt.runset_id
        JOIN hub_owner.sam_sample s
          ON INSTR(',' || rt.sample_list || ',', ',' || s.sample_id || ',') > 0
        WHERE s.last_updated >= v_last_refresh - NUMTODSINTERVAL(v_lookback_minutes, 'MINUTE')

        UNION

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
    );

    SELECT COUNT(*)
    INTO v_changed_count
    FROM onelab_changed_task_plans;

    IF v_changed_count > 0 THEN
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
    END IF;

    UPDATE onelab_report_refresh_state
    SET last_refresh_ts = v_this_refresh
    WHERE id = 1;

    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
    ]',
    start_date      => SYSTIMESTAMP,
    repeat_interval => 'FREQ=MINUTELY;INTERVAL=1',
    enabled         => TRUE
  );
END;
/