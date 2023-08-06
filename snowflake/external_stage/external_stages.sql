USE MANAGE_DB;
CREATE SCHEMA IF NOT EXISTS EXTERNAL_STAGES; 
USE SCHEMA EXTERNAL_STAGES;

---- ## AMAZON METADATA ## ----

CREATE OR replace STAGE EXTERNAL_STAGES.AMAZON_METADATA_STAGE_S3
    URL = 's3://amazon-data-bucket/source-files/amazon_metadata/'
    STORAGE_INTEGRATION = AWS_S3_INTEGRATION; 

LIST @MANAGE_DB.EXTERNAL_STAGES.AMAZON_METADATA_STAGE_S3;

DESC STAGE AMAZON_METADATA_STAGE_S3;


USE SCHEMA EXTERNAL_STAGES;

---- ## AMAZON REVIEWS ## ----

CREATE OR replace STAGE EXTERNAL_STAGES.AMAZON_REVIEWS_STAGE_S3
    URL = 's3://amazon-data-bucket/source-files/amazon_reviews/'
    STORAGE_INTEGRATION = AWS_S3_INTEGRATION; 

LIST @MANAGE_DB.EXTERNAL_STAGES.AMAZON_REVIEWS_STAGE_S3;

DESC STAGE AMAZON_REVIEWS_STAGE_S3;




USE SCHEMA EXTERNAL_STAGES;

---- ## AMAZON REVIEWS STREAMING ## ----

CREATE OR replace STAGE EXTERNAL_STAGES.AMAZON_REVIEWS_STREAMING_STAGE_S3
    URL = 's3://amazon-data-bucket/factored-datathon/amazon_review/'
    STORAGE_INTEGRATION = AWS_S3_INTEGRATION; 

LIST @MANAGE_DB.EXTERNAL_STAGES.AMAZON_REVIEWS_STREAMING_STAGE_S3;

DESC STAGE AMAZON_REVIEWS_STREAMING_STAGE_S3;