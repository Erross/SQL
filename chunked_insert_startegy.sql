SET SERVEROUTPUT ON;

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE onelab_result_report PURGE';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;
/

CREATE TABLE onelab_result_report AS
SELECT *
FROM vw_onelab_result_source
WHERE 1 = 0;

DECLARE
    v_start_id NUMBER := 0;
    v_end_id   NUMBER := 99;
    v_step     NUMBER := 100;
    v_rows     NUMBER;
BEGIN
    LOOP
        INSERT INTO onelab_result_report
        SELECT
            "Task Plan ID",
            "Sample ID",
            "Task ID",
            "Task Name",
            "Task Status",
            "Sample Name",
            "Characteristic",
            "Result",
            "Formatted result",
            "Result entered",
            "Result Source"
        FROM (
            SELECT s.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY
                           "Task Plan ID",
                           "Sample ID",
                           "Characteristic",
                           "Result",
                           "Formatted result",
                           "Result Source"
                       ORDER BY "Result entered" DESC NULLS LAST
                   ) AS rn
            FROM vw_onelab_result_source s
            WHERE TO_NUMBER(REGEXP_SUBSTR("Task Plan ID", '[0-9]+'))
                  BETWEEN v_start_id AND v_end_id
        )
        WHERE rn = 1;

        v_rows := SQL%ROWCOUNT;
        COMMIT;

        DBMS_OUTPUT.PUT_LINE(
            'Loaded Task Plan ID range '
            || v_start_id || ' - ' || v_end_id
            || ': ' || v_rows || ' rows'
        );

        EXIT WHEN v_rows = 0;

        v_start_id := v_start_id + v_step;
        v_end_id   := v_end_id + v_step;
    END LOOP;
END;
/