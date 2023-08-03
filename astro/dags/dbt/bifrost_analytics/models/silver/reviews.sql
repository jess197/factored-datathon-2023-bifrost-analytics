{{ config(materialized='table') }}


WITH bronze_amz_reviews_streaming AS (

 SELECT distinct str_reviews_key AS surr_key_reviews
      , asin::varchar(100) AS asin
      , overall::varchar(5) AS overall
      , review_text::varchar(40000) AS review_text
      , reviewer_id::varchar(100) AS reviewer_id
      , reviewer_name::varchar(20000) reviewer_name
      , summary::varchar(20000) AS summary
      , DATE(TO_TIMESTAMP_NTZ(review_time)) AS review_time
      , verified::boolean AS verified
      , ingestion_date 
    FROM {{ ref('amazon_reviews_streaming') }}

),

silver_amz_reviews AS (

   SELECT
      *
   FROM bronze_amz_reviews_streaming

   WHERE ingestion_date > ( select max(ingestion_date) from silver_amz_reviews )

)

SELECT * FROM silver_amz_reviews