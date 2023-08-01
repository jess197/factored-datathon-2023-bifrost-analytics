{{ config(materialized='table') }}

with source_amz_metadata as (
  select raw_file, last_modified_date
    from raw.metadata
)
select REGEXP_REPLACE(REGEXP_REPLACE($1:title, '&quot;', '"'), '&amp;', 'and') AS title
     , $1:asin::varchar(100) as asin 
     , $1:brand::varchar(1000) as brand 
     , REGEXP_REPLACE(ARRAY_TO_STRING($1:category, ', '), '&amp;', 'and') AS category
     , $1:date as date 
     , REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(ARRAY_TO_STRING($1:description, ', '), '&amp;', 'and'), '&quot;', '"'),'<br>','\n')  AS description
     , TRY_CAST(REGEXP_REPLACE($1:price, '[^0-9.]', '') AS FLOAT) AS price
     , $1:details as details
     , $1:feature as feature 
     , CASE WHEN $1:main_cat LIKE '%<img src%' THEN REGEXP_REPLACE(REGEXP_SUBSTR($1:main_cat, 'alt="([^"]*)"'), '^alt="|"$', '')
       ELSE $1:main_cat
       END as main_category  
     , ARRAY_TO_STRING($1:also_buy, ', ') as also_buy
     , ARRAY_TO_STRING($1:also_view, ', ') as also_view
     , $1:rank::varchar(1000) as rank 
     , $1:similar_item::varchar(100000) as similar_item
     , $1:image::varchar(10000) as image 
     , $1:fit::varchar(10000) as fit
     , $1:tech1::varchar(10000) as tech1
     , $1:tech2::varchar(10000) as tech2
     , last_modified_date as ingestion_date 
     , {{ dbt_utils.generate_surrogate_key(['asin','also_buy', 'also_view']) }} as metadata_key
  from source_amz_metadata