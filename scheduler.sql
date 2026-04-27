BEGIN
    DBMS_SCHEDULER.CREATE_JOB (
        job_name        => 'ONELAB_RESULT_REPORT_REFRESH_JOB',
        job_type        => 'STORED_PROCEDURE',
        job_action      => 'REFRESH_ONELAB_RESULT_REPORT',
        start_date      => SYSTIMESTAMP,
        repeat_interval => 'FREQ=MINUTELY;INTERVAL=1',
        enabled         => TRUE
    );
END;
/