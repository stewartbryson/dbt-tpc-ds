
WITH catalog_sales AS (
    select * from {{ source('tpcds', 'catalog_sales') }}
),
store_sales AS (
    select * from {{ source('tpcds', 'store_sales') }}
),
date_dim AS (
    select * from {{ source('tpcds', 'date_dim') }}
),
store AS (
    select * from {{ source('tpcds', 'store') }}
),
item AS (
    select * from {{ source('tpcds', 'item') }}
)
SELECT * from
    (SELECT i_category, i_class, i_brand, s_store_name, s_company_name, d_moy, sum(ss_sales_price) sum_sales, avg(sum(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name) avg_monthly_sales
     FROM item, store_sales, date_dim, store
     WHERE ss_item_sk = i_item_sk
       AND ss_sold_date_sk = d_date_sk
       AND ss_store_sk = s_store_sk
       AND d_year = 1999
       AND ((i_category IN ('Books','Electronics','Sports')
         AND i_class IN ('computers','stereo','football') )
         OR (i_category IN ('Men','Jewelry','Women')
             AND i_class IN ('shirts','birdal','dresses')))
     GROUP BY i_category, i_class, i_brand, s_store_name, s_company_name, d_moy) tmp1
WHERE CASE
          WHEN (avg_monthly_sales <> 0) THEN (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales)
          ELSE NULL
          END > 0.1
ORDER BY sum_sales - avg_monthly_sales,
         s_store_name, 1, 2, 3, 5, 6, 7, 8
    LIMIT 100