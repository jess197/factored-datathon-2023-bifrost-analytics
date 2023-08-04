
{{ config(materialized='incremental') }}

with

reviews as (
    select
        SURR_KEY_REVIEWS
        , ASIN
        , OVERALL
        , REVIEWER_ID
        , REVIEW_TEXT
        , REVIEW_TIME
    from {{ref('reviews')}}
    
    {% if is_incremental()%}
        where ingestion_date = ( select max(ingestion_date) from {{ref('reviews')}})
    {% endif %}
)

select 
    distinct *
from
    reviews