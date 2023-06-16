
WITH catalog_sales AS (
    select * from {{ source('tpcds', 'catalog_sales') }}
),
store_sales AS (
    select * from {{ source('tpcds', 'store_sales') }}
),
date_dim AS (
    select * from {{ source('tpcds', 'date_dim') }}
),
time_dim AS (
    select * from {{ source('tpcds', 'time_dim') }}
),
store AS (
    select * from {{ source('tpcds', 'store') }}
),
household_demographics AS (
    select * from {{ source('tpcds', 'household_demographics') }}
)
SELECT count(*)
FROM store_sales ,
     household_demographics,
     time_dim,
     store
WHERE ss_sold_time_sk = time_dim.t_time_sk
  AND ss_hdemo_sk = household_demographics.hd_demo_sk
  AND ss_store_sk = s_store_sk
  AND time_dim.t_hour = 20
  AND time_dim.t_minute >= 30
  AND household_demographics.hd_dep_count = 7
  AND store.s_store_name = 'ese'
ORDER BY count(*)
    LIMIT 100