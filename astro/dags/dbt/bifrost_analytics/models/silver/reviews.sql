{{ config(materialized='incremental') }}


WITH bronze_amz_reviews_streaming AS (

 SELECT str_reviews_key as surr_key_reviews
      , asin::varchar(100) as asin
      , overall::varchar(5) as overall
      , review_text::varchar(40000) as review_text
      , reviewer_id::varchar(100) as reviewer_id
      , reviewer_name::varchar(20000) reviewer_name
      , summary::varchar(20000) AS summary
      , DATE(TO_TIMESTAMP_NTZ(review_time)) AS review_time
      , verified::boolean AS verified
      , ingestion_date 
    FROM {{ ref('amazon_reviews_streaming') }}

),

silver_amz_reviews AS (


   {% if not is_incremental() %}

      SELECT reviews_key as surr_key_reviews
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

    UNION
   {% endif %}
   SELECT
      *
   FROM bronze_amz_reviews_streaming

   {% if is_incremental () %}
      WHERE ingestion_date > ( select max(ingestion_date) from amz_data_silver.reviews )
   {% endif %}

)

SELECT distinct  * FROM silver_amz_reviews

{% if target.name == 'dev' %}

    limit 10000
    
{% end if %}