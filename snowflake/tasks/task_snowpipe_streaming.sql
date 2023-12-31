USE MANAGE_DB;
CREATE SCHEMA TASKS;
USE MANAGE_DB.TASKS;

CREATE OR REPLACE TASK UNPAUSE_PIPE_AMAZON_REVIEWS_STREAMING_TASK
WAREHOUSE = 'INGESTION_WH'
SCHEDULE = 'USING CRON 0 */1 * * * UTC'
AS
ALTER PIPE MANAGE_DB.PIPES.AMAZON_REVIEWS_STREAMING_RAW SET PIPE_EXECUTION_PAUSED = false;


CREATE OR REPLACE TASK REFRESH_PIPE_AMAZON_REVIEWS_STREAMING_TASK
WAREHOUSE = 'INGESTION_WH'
SCHEDULE = 'USING CRON 1 */1 * * * UTC'
AS
ALTER PIPE MANAGE_DB.PIPES.AMAZON_REVIEWS_STREAMING_RAW REFRESH;

SHOW TASKS;

ALTER TASK UNPAUSE_PIPE_AMAZON_REVIEWS_STREAMING_TASK RESUME;
ALTER TASK REFRESH_PIPE_AMAZON_REVIEWS_STREAMING_TASK RESUME;

ALTER TASK UNPAUSE_PIPE_AMAZON_REVIEWS_STREAMING_TASK SUSPEND;
ALTER TASK REFRESH_PIPE_AMAZON_REVIEWS_STREAMING_TASK SUSPEND;