{{ config(materialized='table') }}

with

simmilarity_rec as (
    SELECT sr.product_id
         , srec.value::varchar as recommendations
         , main_category
      FROM AMZ_DATA_SILVER.SIMILARITY_RECOMMENDATIONS sr,
    LATERAL FLATTEN(input => PARSE_JSON(sr.recommendations)) srec
    
)

select 
    distinct *
from
    simmilarity_rec

