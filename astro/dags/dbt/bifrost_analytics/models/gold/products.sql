
{{ config(materialized='incremental') }}

WITH 
products_intermed as
(
    select 
        metadata_key
        , title
        , asin
        , brand
        , date
        , price
        , main_category
    from {{ ref('metadata') }}

{% if is_incremental() %}

    where ingestion_date > (
        SELECT last_altered
        FROM {{ database }}.information_schema.tables
        WHERE table_schema = 'AMZ_DATA_GOLD' AND table_name = 'PRODUCTS'
     )

{% endif %}


)

select
    distinct *
from
    products_intermed

