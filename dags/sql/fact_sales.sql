CREATE OR REPLACE TABLE datawarehouse-411117.OLAP.Fact_sales AS
SELECT
  od.orderID,
  od.productID,
  o.customerID,
  o.employeeID,
  p.categoryID,
  od.quantity,
  od.unitPrice,
  od.discount,
  CAST(FORMAT_DATE('%Y%m%d', o.orderDate) AS INT64) AS date_key,
  o.freight,
  CURRENT_TIMESTAMP() AS insertion_timestamp
FROM
  `datawarehouse-411117.OLTP.order-details` od
LEFT JOIN `datawarehouse-411117.OLTP.orders` o
ON od.orderID = o.orderID
LEFT JOIN `datawarehouse-411117.OLTP.products` p
ON od.productID= p.productID
WHERE od.orderID IS NOT NULL
QUALIFY ROW_NUMBER() OVER(PARTITION BY o.orderID, od.productID) = 1;
