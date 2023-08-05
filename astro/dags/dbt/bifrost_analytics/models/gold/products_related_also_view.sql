
{{ config(materialized='incremental') }}

with

metadata_related_also_view as (
    select metadata_key
         , asin
         , av.value::varchar as also_view
    from {{ref('metadata')}},
    LATERAL FLATTEN(input => also_view) av
    
    {% if is_incremental()%}
        where ingestion_date > (
            SELECT last_altered
            FROM {{ database }}.information_schema.tables
            WHERE table_schema = 'AMZ_DATA_GOLD' AND table_name = 'PRODUCTS_RELATED_ALSO_VIEW'
        )
    {% endif %}
)

select 
    distinct *
from
    metadata_related_also_view

