-- PROCEDURE TO UPDATE PRICES BASED AVG PRICES FROM MAIN CATEGORIES

CREATE OR REPLACE PROCEDURE update_prices_based_on_category_average()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
    $$
    BEGIN
        CREATE OR REPLACE TEMPORARY TABLE avg_prices AS (
             SELECT
                  main_category as cat,
                  ROUND(AVG(price),2) AS avg_price
                FROM
                  amazon.amz_data_gold.products
                WHERE
                  price IS NOT NULL
                GROUP BY
                  cat
              );
      -- Step 2: Update the NULL price values based on the category average
      UPDATE
        amazon.amz_data_gold.products t
      SET
        price = ap.avg_price
      FROM
        avg_prices ap
      WHERE
        t.price IS NULL
        AND t.main_category = ap.cat;
    
      RETURN 'Procedure executed successfully.';
    END;
    $$;

-- To call the procedure, simply execute it:
CALL update_prices_based_on_category_average(); 



CREATE OR REPLACE PROCEDURE update_prices_with_no_price_in_category_to_zero()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
    $$
    BEGIN
        CREATE OR REPLACE TEMPORARY TABLE null_prices AS (
               SELECT metadata_key
                FROM
                  amazon.amz_data_gold.products
                WHERE
                  price IS NULL
              );
      -- Step 2: Update the NULL price values based on the category average
      UPDATE
        amazon.amz_data_gold.products t
      SET
        price = 0
      FROM
        null_prices np
      WHERE
        t.price IS NULL
        AND t.metadata_key = np.metadata_key;
    
      RETURN 'Procedure executed successfully.';
    END;
    $$;
CALL update_prices_with_no_price_in_category_to_zero();