-- Number of recomendations per review per reviewer 
WITH REVIEWERS AS (
    SELECT DISTINCT REVIEWER_ID, 
                    REVIEWER_NAME
    FROM AMZ_DATA_SILVER.REVIEWS
)

SELECT  PR.REVIEWER_ID,
        REV.REVIEWER_NAME,
        PR.PRODUCT_ID,
        PRD.TITLE,
        COUNT(RPD.TITLE) total_recommendations
  FROM  AMZ_DATA_GOLD.PERSONALIZED_RECOMMENDATIONS PR 
  JOIN  AMZ_DATA_GOLD.PRODUCTS PRD ON PR.PRODUCT_ID = PRD.ASIN 
  JOIN  AMZ_DATA_GOLD.PRODUCTS RPD ON PR.RECOMMENDATIONS = RPD.ASIN 
  JOIN  REVIEWERS REV ON PR.REVIEWER_ID = REV.REVIEWER_ID
  GROUP BY ALL



-- What are these recommendations ? 
WITH REVIEWERS AS (
    SELECT DISTINCT REVIEWER_ID
    FROM AMZ_DATA_SILVER.REVIEWS
)
SELECT RPD.TITLE as product_recommendation
  FROM AMZ_DATA_GOLD.PERSONALIZED_RECOMMENDATIONS PR 
  JOIN AMZ_DATA_GOLD.PRODUCTS PRD ON PR.PRODUCT_ID = PRD.ASIN 
  JOIN AMZ_DATA_GOLD.PRODUCTS RPD ON PR.RECOMMENDATIONS = RPD.ASIN 
  JOIN REVIEWERS REV ON PR.REVIEWER_ID = REV.REVIEWER_ID
 WHERE PR.REVIEWER_ID = 'AUGLDKIT5LZZ7'
 AND PR.PRODUCT_ID = 'B015HHY17I'