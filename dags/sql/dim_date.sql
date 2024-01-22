CREATE OR REPLACE TABLE `datawarehouse-411117.OLAP.Dim_date` AS
SELECT
    CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) AS date_key,
    date AS full_date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(QUARTER FROM date) AS quarter,
    CONCAT('Q', CAST(EXTRACT(QUARTER FROM date) AS STRING)) AS quarter_name,
    EXTRACT(MONTH FROM date) AS month,
    FORMAT_DATE('%B', date) AS month_name,
    EXTRACT(WEEK FROM date) AS week,
    EXTRACT(DAY FROM date) AS day,
    FORMAT_DATE('%A', date) AS day_name
FROM
    UNNEST(GENERATE_DATE_ARRAY(DATE '1996-07-04', DATE '2030-12-31', INTERVAL 1 DAY)) AS date;