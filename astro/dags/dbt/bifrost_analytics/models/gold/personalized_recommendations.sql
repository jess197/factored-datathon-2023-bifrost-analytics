{{ config(materialized='table') }}

with

personalized_rec as (
    SELECT pr.reviewer_id
         , pid.value::varchar as product_id
         , prec.value::varchar as recommendations
      FROM AMZ_DATA_SILVER.PERSONALIZED_RECOMMENDATIONS pr,
    LATERAL FLATTEN(input => PARSE_JSON(pr.product_id)) pid,
    LATERAL FLATTEN(input => PARSE_JSON(pr.recommendations)) prec
    
)

select 
    distinct *
from
    personalized_rec

