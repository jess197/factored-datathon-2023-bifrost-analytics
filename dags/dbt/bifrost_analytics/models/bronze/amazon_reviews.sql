{{ config(materialized='incremental') }}
with source_amz_reviews as (
  select raw_file, last_modified_date
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
      , last_modified_date as ingestion_date 
      , {{ dbt_utils.generate_surrogate_key(['review_text','reviewer_id','asin','review_time']) }} as reviews_key
   FROM source_amz_reviews