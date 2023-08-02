{{ config(materialized='incremental') }}


with bronze_amz_reviews as
(

 SELECT distinct reviews_key as surr_key_reviews
      , asin::varchar(100) as asin
      , overall::varchar(5) as overall
      , review_text::varchar(40000) as review_text
      , reviewer_id::varchar(100) as reviewer_id
      , reviewer_name::varchar(20000) reviewer_name
      , summary::varchar(20000) as summary
      , DATE(TO_TIMESTAMP_NTZ(review_time)) as review_time
      , verified::boolean as verified
      , ingestion_date 
    FROM {{ ref('amazon_reviews') }}

), 

bronze_amz_reviews_streaming as (

 SELECT distinct str_reviews_key as surr_key_reviews
      , asin::varchar(100) as asin
      , overall::varchar(5) as overall
      , review_text::varchar(40000) as review_text
      , reviewer_id::varchar(100) as reviewer_id
      , reviewer_name::varchar(20000) reviewer_name
      , summary::varchar(20000) as summary
      , DATE(TO_TIMESTAMP_NTZ(review_time)) as review_time
      , verified::boolean as verified
      , ingestion_date 
    FROM {{ ref('amazon_reviews_streaming') }}

),

silver_amz_reviews(

   SELECT 
      *
   FROM bronze_amz_reviews
   WHERE
      review_date > (SELECT MAX(review_date) FROM AMZ_DATA_SILVER.REVIEWS )
  UNION 
   SELECT
      *
   FROM bronze_amz_reviews_streaming
   WHERE
      review_date >= (SELECT MAX(review_date) FROM AMZ_DATA_SILVER.REVIEWS )

)

SELECT * FROM silver_amz_reviews