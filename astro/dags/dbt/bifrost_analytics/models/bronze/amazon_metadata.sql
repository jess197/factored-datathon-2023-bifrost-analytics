{{ config(materialized='table') }}

WITH source_amz_metadata AS (

  SELECT raw_file, last_modified_date
    FROM raw.metadata

),

amz_metadata AS (

  SELECT REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE($1:title, '&quot;', '"'), '&amp;', 'and'), '&#39;', '\'', 1, 0, 'i') AS title
      , $1:asin::varchar(100) AS asin 
      , $1:brand::varchar(1000) AS brand 
      , REGEXP_REPLACE(ARRAY_TO_STRING($1:category, ', '), '&amp;', 'and') AS category
      , $1:date AS date 
      , REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(ARRAY_TO_STRING($1:description, ', '), '&amp;', 'and'), '&quot;', '"'),'<br>','\n')  AS description
      , TRY_CAST(REGEXP_REPLACE($1:price, '[^0-9.]', '') AS FLOAT) AS price
      , $1:details AS details
      , ARRAY_TO_STRING($1:feature, ', ') AS feature 
      , CASE WHEN $1:main_cat LIKE '%<img src%' THEN REGEXP_REPLACE(REGEXP_SUBSTR($1:main_cat, 'alt="([^"]*)"'), '^alt="|"$', '')
        ELSE $1:main_cat
        END AS main_category  
      , $1:also_buy AS also_buy
      , $1:also_view AS also_view
      ,   CASE WHEN $1:rank IS NULL OR $1:rank = '[]' THEN ''::VARCHAR
              ELSE REPLACE(REGEXP_REPLACE($1:rank, '[>#()[""]', '', 1, 0, 'i'),']','')::VARCHAR(5000) END AS rank
      , $1:similar_item::varchar(100000) AS similar_item
      , $1:image::varchar(10000) AS image 
      , $1:fit::varchar(10000) AS fit
      , $1:tech1::varchar(10000) AS tech1
      , $1:tech2::varchar(10000) AS tech2
      , last_modified_date AS ingestion_date 
      , {{ dbt_utils.generate_surrogate_key(['asin','also_buy', 'also_view']) }} AS metadata_key
    FROM source_amz_metadata

)

SELECT DISTINCT * FROM amz_metadata


