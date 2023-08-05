
{{ config(materialized='incremental') }}

with

metadata_related_also_buy as (
    select metadata_key
         , asin
         , ab.value::varchar as also_buy
      from {{ref('metadata')}},
      LATERAL FLATTEN(input =>also_buy) ab
    
    {% if is_incremental()%}
        where ingestion_date > (
            SELECT last_altered
            FROM {{ database }}.information_schema.tables
            WHERE table_schema = 'AMZ_DATA_GOLD' AND table_name = 'PRODUCTS_RELATED_ALSO_BUY'
        )
    {% endif %}
)

select 
    distinct *
from
    metadata_related_also_buy

