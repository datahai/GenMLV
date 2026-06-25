COMMENT "Generated sample for Fabric MLV Manager"
TBLPROPERTIES ("data_zone"="gold")
AS
SELECT
  ProductID,
  ProductName,
  Category,
  SilverLastUpdated
FROM silver.mlv_product
