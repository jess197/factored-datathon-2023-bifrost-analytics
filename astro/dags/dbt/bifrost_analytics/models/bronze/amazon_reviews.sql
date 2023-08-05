{{ config(materialized='table') }}

with source_amz_reviews AS (

  select raw_file, last_modified_date
    from raw.reviews

),

amz_reviews AS (
  
 SELECT $1:asin AS asin 
      , $1:overall AS overall
      , $1:reviewText AS review_text
      , $1:reviewerID AS reviewer_id
      , $1:reviewerName AS reviewer_name
      , $1:style AS style 
      , $1:summary AS summary
      , $1:unixReviewTime AS review_time
      , $1:verified AS verified
      , $1:vote AS vote
      , last_modified_date AS ingestion_date 
      , {{ dbt_utils.generate_surrogate_key(['review_text','reviewer_id','asin','review_time']) }} AS reviews_key
   FROM source_amz_reviews
)

SELECT DISTINCT * FROM amz_reviews
