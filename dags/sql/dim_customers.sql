CREATE OR REPLACE TABLE
  datawarehouse-411117.OLAP.Dim_customers AS
SELECT
  customerID,
  companyName,
  contactName,
  contactTitle,
  address,
  city,
  region,
  postalCode,
  country,
  phone,
  fax,
  current_timestamp() as  insertion_timestamp,
FROM
  datawarehouse-411117.OLTP.customers
QUALIFY ROW_NUMBER() OVER(PARTITION BY customerID) = 1;