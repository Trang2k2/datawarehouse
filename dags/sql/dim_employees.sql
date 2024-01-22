CREATE OR REPLACE TABLE
  datawarehouse-411117.OLAP.Dim_employees AS
SELECT
  employeeID,
  lastName,
  firstName,
  title,
  titleOfCourtesy,
  birthDate,
  hireDate,
  address,
  city,
  region,
  postalCode,
  country,
  homePhone,
  extension,
  photo,
  notes,
  reportsTo,
  photoPath,
  current_timestamp() as  insertion_timestamp,
FROM
  datawarehouse-411117.OLTP.employees
QUALIFY ROW_NUMBER() OVER(PARTITION BY employeeID) = 1;