
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
customer AS (
    select * from {{ source('tpcds', 'customer') }}
),
web_sales AS (
    select * from {{ source('tpcds', 'web_sales') }}
)
SELECT count(*)
FROM ((SELECT DISTINCT c_last_name,
                       c_first_name,
                       d_date
       FROM store_sales,
            date_dim,
            customer
       WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
         AND store_sales.ss_customer_sk = customer.c_customer_sk
         AND d_month_seq BETWEEN 1200 AND 1200+11)
      EXCEPT
      (SELECT DISTINCT c_last_name,
                       c_first_name,
                       d_date
       FROM catalog_sales,
            date_dim,
            customer
       WHERE catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
         AND catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
         AND d_month_seq BETWEEN 1200 AND 1200+11)
      EXCEPT
      (SELECT DISTINCT c_last_name,
                       c_first_name,
                       d_date
       FROM web_sales,
            date_dim,
            customer
       WHERE web_sales.ws_sold_date_sk = date_dim.d_date_sk
         AND web_sales.ws_bill_customer_sk = customer.c_customer_sk
         AND d_month_seq BETWEEN 1200 AND 1200+11)) cool_cust