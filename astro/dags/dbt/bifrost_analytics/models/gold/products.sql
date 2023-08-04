
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

    where ingestion_date = (select max(ingestion_date) from amz_data_gold.products)

{% endif %}


)

select
    distinct *
from
    products_intermed

