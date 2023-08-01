{{ config(materialized='table') }}
with bronze_amz_reviews as (
  select reviews_key
       , asin 
       , overall 
       , review_text
       , reviewer_id
       , reviewer_name
       , summary
       , review_time 
       , verified
       , ingestion_date
    FROM {{ ref('amazon_reviews') }}
), bronze_amz_reviews_streaming as (
    select str_reviews_key 
       , asin 
       , overall 
       , review_text
       , reviewer_id
       , reviewer_name
       , summary
       , review_time 
       , verified
       , ingestion_date
    FROM {{ ref('amazon_reviews_streaming') }}

)
 SELECT distinct reviews_key as surr_key_reviews
      , asin::varchar(100) as asin
      , overall::varchar(5) as overall
      , review_text::varchar(40000) as review_text
      , reviewer_id::varchar(100) as reviewer_id
      , reviewer_name::varchar(20000) reviewer_name
      , summary::varchar(20000) as summary
      , DATE(TO_TIMESTAMP_NTZ(review_time)) as review_time
      , verified::varchar(20000) as verified
      , ingestion_date 
   FROM bronze_amz_reviews
  UNION 
 SELECT distinct str_reviews_key as surr_key_reviews
      , asin::varchar(100) as asin
      , overall::varchar(5) as overall
      , review_text::varchar(40000) as review_text
      , reviewer_id::varchar(100) as reviewer_id
      , reviewer_name::varchar(20000) reviewer_name
      , summary::varchar(20000) as summary
      , DATE(TO_TIMESTAMP_NTZ(review_time)) as review_time
      , verified::varchar(20000) as verified
      , ingestion_date 
   FROM bronze_amz_reviews_streaming
  