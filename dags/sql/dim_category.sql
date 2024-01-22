CREATE OR REPLACE TABLE
  `datawarehouse-411117.OLAP.Dim_categories` AS
SELECT
  categoryID,
  categoryName,
  description,
  current_timestamp() as  insertion_timestamp,
FROM
  `datawarehouse-411117.OLTP.categories`
QUALIFY ROW_NUMBER() OVER(PARTITION BY categoryID) = 1;