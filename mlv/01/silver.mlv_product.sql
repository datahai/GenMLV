COMMENT "Generated sample for Fabric MLV Manager"
TBLPROPERTIES ("data_zone"="silver")
AS
SELECT
  ProductID,
  ProductName,
  Category,
  CURRENT_TIMESTAMP() AS SilverLastUpdated
FROM Product
