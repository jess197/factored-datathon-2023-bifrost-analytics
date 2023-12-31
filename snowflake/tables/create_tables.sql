----- ### AMAZON DEV ## ------ 
CREATE DATABASE AMAZON_DEV; 

---- ## AMAZON METADATA ## ----
CREATE DATABASE AMAZON;
USE AMAZON;
CREATE SCHEMA AMAZON.RAW;
USE SCHEMA AMAZON.RAW;

SELECT * FROM @MANAGE_DB.EXTERNAL_STAGES.AMAZON_METADATA_STAGE_S3
(file_format => MANAGE_DB.FILE_FORMATS.JSON_FORMAT);

CREATE OR REPLACE TABLE AMAZON.RAW.METADATA(
RAW_FILE VARIANT,LAST_MODIFIED_DATE timestamp_ntz, FILE_NAME varchar);
-- HIGHLY COMPRESSED, USED FOR SEMI-STRUCTURED, OPTIMIZES QUERIES IN SNOWFLAKE.

SELECT * 
  FROM AMAZON.RAW.METADATA;


---- ## AMAZON REVIEWS ## ----
USE AMAZON;
USE SCHEMA AMAZON.RAW;

SELECT * FROM @MANAGE_DB.EXTERNAL_STAGES.AMAZON_REVIEWS_STAGE_S3
(file_format => MANAGE_DB.FILE_FORMATS.JSON_FORMAT);

CREATE OR REPLACE TABLE AMAZON.RAW.REVIEWS(
RAW_FILE VARIANT,LAST_MODIFIED_DATE timestamp_ntz, FILE_NAME varchar);
-- HIGHLY COMPRESSED, USED FOR SEMI-STRUCTURED, OPTIMIZES QUERIES IN SNOWFLAKE.

SELECT * 
  FROM AMAZON.RAW.REVIEWS;


---- ## AMAZON REVIEWS STREAMING ## ----
USE AMAZON;
USE SCHEMA AMAZON.RAW;

SELECT METADATA$FILENAME FROM @MANAGE_DB.EXTERNAL_STAGES.AMAZON_REVIEWS_STREAMING_STAGE_S3
(file_format => MANAGE_DB.FILE_FORMATS.JSON_FORMAT);

CREATE OR REPLACE TABLE AMAZON.RAW.REVIEWS_STREAMING(
RAW_FILE VARIANT,LAST_MODIFIED_DATE timestamp_ntz, FILE_NAME varchar);
-- HIGHLY COMPRESSED, USED FOR SEMI-STRUCTURED, OPTIMIZES QUERIES IN SNOWFLAKE.




