CREATE OR REPLACE TABLE
  `datawarehouse-411117.OLAP.Dim_products` AS
SELECT
  productID,
  productName,
  supplierID,
  categoryID,
  quantityPerUnit,
  unitPrice,
  unitsInStock,
  unitsOnOrder,
  reorderLevel,
  discontinued,
  current_timestamp() as  insertion_timestamp,
FROM
  `datawarehouse-411117.OLTP.products`
QUALIFY ROW_NUMBER() OVER(PARTITION BY productID) = 1;
