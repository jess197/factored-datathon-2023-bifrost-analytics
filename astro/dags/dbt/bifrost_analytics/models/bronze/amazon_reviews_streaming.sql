


{{ config(materialized='table') }}

with source_amz_reviews_streaming AS (

    SELECT PARSE_JSON(rf.value), rs.last_modified_date
    FROM AMAZON.RAW.REVIEWS_STREAMING rs,
    LATERAL FLATTEN(input => rs.RAW_FILE) rf
),

amz_reviews_streaming AS (

    SELECT $1:asin AS asin 
        , $1:internal_partition AS internal_partition
        , $1:overall AS overall
        , $1:partition_number AS partition_number 
        , $1:reviewText AS review_text
        , $1:reviewerID AS reviewer_id
        , $1:reviewerName AS reviewer_name
        , $1:style AS style 
        , $1:summary AS summary
        , $1:unixReviewTime AS review_time
        , $1:verified AS verified
        , $1:vote AS vote
        , last_modified_date AS ingestion_date 
        , {{ dbt_utils.generate_surrogate_key(['partition_number','asin']) }} AS str_reviews_key
    FROM source_amz_reviews_streaming

)

SELECT DISTINCT * FROM amz_reviews_streaming
{% if target.name == 'dev' %}

    limit 10000
    
{% end if %}