{{ config(materialized='external', location='s3://datafy-dp-samples-ympfsg/tpcds-dbt-duckdb/q15_100G_result.parquet') }}
WITH catalog_sales AS (
    select * from {{ source('external_source', 'catalog_sales') }}
),
date_dim AS (
    select * from {{ source('external_source', 'date_dim') }}
),
customer AS (
    select * from {{ source('external_source', 'customer') }}
),
customer_address AS (
    select * from {{ source('external_source', 'customer_address') }}
)

SELECT ca_zip,
       sum(cs_sales_price)
FROM catalog_sales,
     customer,
     customer_address,
     date_dim
WHERE cs_bill_customer_sk = c_customer_sk
  AND c_current_addr_sk = ca_address_sk
  AND (SUBSTRING(ca_zip, 1, 5) IN ('85669',
                                   '86197',
                                   '88274',
                                   '83405',
                                   '86475',
                                   '85392',
                                   '85460',
                                   '80348',
                                   '81792')
    OR ca_state IN ('CA',
                    'WA',
                    'GA')
    OR cs_sales_price > 500)
  AND cs_sold_date_sk = d_date_sk
  AND d_qoy = 2
  AND d_year = 2001
GROUP BY ca_zip
ORDER BY ca_zip NULLS FIRST
    LIMIT 100