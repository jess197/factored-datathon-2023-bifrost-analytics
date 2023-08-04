
{{ config(materialized='incremental') }}

with
prd_details_base as
(

    select 
        DISTINCT * 
    from 
        amz_data_silver.metadata cte
    , table (flatten(cte.details))

),

details_values as 
(
    select
        metadata_key
        , asin
        , REPLACE(REGEXP_REPLACE(TRIM(KEYS),'    '),':') details_key,
        , trim(REPLACE(REGEXP_REPLACE(VALUE,'^["!@#$%¨&*_+|;?:\\\s]*','',1),'"','')) details_values
        , ingestion_date
    from
        prd_details_base
    where 
    lower(REPLACE(REGEXP_REPLACE(TRIM(KEYS),'    '),':')) <> 'asin'

    {% if is_incremental() %}
    
        AND ingestion_date = (select max(ingestion_date) from details_values )

    {% ifend %}
)

    select 
        distinct * 
    from
        details_values

