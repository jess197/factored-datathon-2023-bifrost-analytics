
{{ config(materialized='incremental') }}

with

metadata_related as (
    select
        metadata_key
        , ASIN
        , ALSO_BUY
        , ALSO_VIEW
        , CATEGORY AS RELATED_CATEGORIES
    from {{ref('metadata')}}
    
    {% if is_incremental()%}
        where ingestion_date > (
            SELECT last_altered
            FROM {{ database }}.information_schema.tables
            WHERE table_schema = 'AMZ_DATA_GOLD' AND table_name = 'PRODUCTS_RELATED'
        )
    {% endif %}
)

select 
    distinct *
from
    metadata_related

{% if target.name == 'dev' %}

    limit 10000
    
{% end if %}