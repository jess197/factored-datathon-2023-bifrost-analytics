


{{ config(materialized='incremental') }}

with source_amz_reviews_streaming as (
    SELECT PARSE_JSON(rf.value), rs.last_modified_date
    FROM AMAZON.RAW.REVIEWS_STREAMING rs,
    LATERAL FLATTEN(input => rs.RAW_FILE) rf
)
 SELECT $1:asin as asin 
      , $1:internal_partition as internal_partition
      , $1:overall as overall
      , $1:partition_number as partition_number 
      , $1:reviewText as review_text
      , $1:reviewerID as reviewer_id
      , $1:reviewerName as reviewer_name
      , $1:style as style 
      , $1:summary as summary
      , $1:unixReviewTime as review_time
      , $1:verified as verified
      , $1:vote as vote
      , last_modified_date as ingestion_date 
      , {{ dbt_utils.generate_surrogate_key(['partition_number','asin']) }} as str_reviews_key
   FROM source_amz_reviews_streaming