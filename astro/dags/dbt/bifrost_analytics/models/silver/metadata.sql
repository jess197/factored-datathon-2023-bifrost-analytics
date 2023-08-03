WITH bronze_amz_metadata AS (

  SELECT metadata_key
       , title 
       , asin 
       , CASE WHEN REGEXP_LIKE(brand, '^Visit Amazon''s .* Page$', 'i') THEN
                   REGEXP_REPLACE(brand, '^Visit Amazon''s (.*) Page$', '\\1', 1, 0, 'i') 
         ELSE brand
         END AS brand_product
       , category 
       , CASE
        WHEN REGEXP_LIKE(date, '<.*>', 'i') THEN null  -- Set HTML content to NULL
        ELSE date::varchar(10000)   
        END AS date_metadata
      , price 
      , details 
      , feature 
      , INITCAP(replace(main_category, '&amp;', '&')) AS main_category
      , also_buy
      , also_view
      , rank
  FROM {{ ref('amazon_metadata') }}

),

amz_metada_silver AS (

  SELECT DISTINCT metadata_key
    , REGEXP_REPLACE(title, '&rsquo;', '''') AS title
    , asin
    , brand_product AS brand 
    , category 
    , date_metadata AS date 
    , price 
    , details 
    , feature 
    , INITCAP(replace(main_category, '&amp;', '&')) AS main_category
    , also_buy
    , also_view
    , rank
  FROM bronze_amz_metadata

)

SELECT * FROM amz_metada_silver