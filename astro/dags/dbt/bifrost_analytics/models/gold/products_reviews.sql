
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
        where ingestion_date > (
            SELECT last_altered
            FROM {{ database }}.information_schema.tables
            WHERE table_schema = 'AMZ_DATA_GOLD' AND table_name = 'PRODUCTS_REVIEWS'
        )
    {% endif %}
)

select 
    distinct *
from
    reviews

{% if target.name == 'dev' %}

    limit 10000
    
{% endif %}