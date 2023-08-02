{{ config(materialized='table') }}

with bronze_amz_metadata AS (

  SELECT metadata_key
       , title 
       , asin 
       , CASE
           WHEN REGEXP_LIKE(brand, '^Visit Amazon''s\s+.*Page$', 'i') THEN REGEXP_REPLACE(brand, '^Visit Amazon''s\s+| Page$', '')  -- Remove "Visit Amazon's " from the beginning and " Page" from the end of the brand
           ELSE brand
         END AS brand_product
       , category 
       , CASE
        WHEN REGEXP_LIKE(date, '<.*>', 'i') THEN null  -- Set HTML content to NULL
        ELSE date::varchar(10000)   -- Return the original date as is
        END AS date_metadata
      , price 
      , details 
      , feature 
      , main_category  
      , also_buy
      , also_view
      , rank
      , ingestion_date
  FROM {{ ref('amazon_metadata') }}

),

amz_metada_silver as (

  SELECT distinct metadata_key
    , REGEXP_REPLACE(title, '&rsquo;', '''') AS title
    , asin
    , brand_product as brand 
    , category 
    , date_metadata as date 
    , price 
    , details 
    , feature 
    , main_category  
    , also_buy
    , also_view
    , rank
    , ingestion_date
  FROM bronze_amz_metadata

)

SELECT * FROM amz_metada_silver