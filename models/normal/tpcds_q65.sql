{{ config(materialized='external', location='s3://datafy-dp-samples-ympfsg/tpcds-dbt-duckdb/q65_100G_result.parquet') }}

WITH catalog_sales AS (
    select * from {{ source('external_source', 'catalog_sales') }}
),
store_sales AS (
    select * from {{ source('external_source', 'store_sales') }}
),
date_dim AS (
    select * from {{ source('external_source', 'date_dim') }}
),
store AS (
    select * from {{ source('external_source', 'store') }}
),
item AS (
    select * from {{ source('external_source', 'item') }}
)
SELECT s_store_name,
       i_item_desc,
       sc.revenue,
       i_current_price,
       i_wholesale_cost,
       i_brand
FROM store,
     item,
     (SELECT ss_store_sk,
             avg(revenue) AS ave
      FROM
          (SELECT ss_store_sk,
                  ss_item_sk,
                  sum(ss_sales_price) AS revenue
           FROM store_sales,
                date_dim
           WHERE ss_sold_date_sk = d_date_sk
             AND d_month_seq BETWEEN 1176 AND 1176+11
           GROUP BY ss_store_sk,
                    ss_item_sk) sa
      GROUP BY ss_store_sk) sb,
     (SELECT ss_store_sk,
             ss_item_sk,
             sum(ss_sales_price) AS revenue
      FROM store_sales,
           date_dim
      WHERE ss_sold_date_sk = d_date_sk
        AND d_month_seq BETWEEN 1176 AND 1176+11
      GROUP BY ss_store_sk,
               ss_item_sk) sc
WHERE sb.ss_store_sk = sc.ss_store_sk
  AND sc.revenue <= 0.1 * sb.ave
  AND s_store_sk = sc.ss_store_sk
  AND i_item_sk = sc.ss_item_sk
ORDER BY s_store_name NULLS FIRST,
         i_item_desc NULLS FIRST
    LIMIT 100