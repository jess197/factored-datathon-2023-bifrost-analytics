
with
prd_details_base as
(

    select 
        DISTINCT * 
    from 
        {{ref('metadata')}} cte
    , table (flatten(cte.details))

),

details_values as 
(
    select
        metadata_key
        , asin
        , REPLACE(REGEXP_REPLACE(TRIM(KEY),'    '),':') details
        , trim(REPLACE(REGEXP_REPLACE(VALUE,'^["!@#$%¨&*_+|;?:\\\s]*','',1),'"','')) details_values
    from
        prd_details_base
    where 
        lower(REPLACE(REGEXP_REPLACE(TRIM(KEY),'    '),':')) <> 'asin'

    {% if is_incremental() %}
    
        AND 
            ingestion_date > (
            SELECT last_altered
            FROM {{ database }}.information_schema.tables
            WHERE table_schema = 'AMZ_DATA_GOLD' AND table_name = 'PRODUCTS_DETAILS'
        )

    {% endif %}
),

details_with_key as 
(
    select
        *,
        {{ dbt_utils.generate_surrogate_key(['METADATA_KEY','DETAILS', 'ASIN']) }} AS details_keys
    from 
        details_values
)

    select 
        distinct * 
    from
        details_with_key

