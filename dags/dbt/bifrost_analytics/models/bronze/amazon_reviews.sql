{{ config(materialized='table') }}
with source_amz_reviews as (
  select raw_file
    from raw.reviews
)
 SELECT $1:asin as asin 
      , $1:overall as overall
      , $1:reviewText as review_text
      , $1:reviewerID as reviewer_id
      , $1:reviewerName as reviewer_name
      , $1:style as style 
      , $1:summary as summary
      , $1:unixReviewTime as review_time
      , $1:verified as verified
      , $1:vote as vote
      , current_timestamp as ingestion_date 
   FROM source_amz_reviews