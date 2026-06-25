COMMENT "Gold product dimension built using silver product MLV"
TBLPROPERTIES (
  "data_zone"="gold",
  "domain"="ecommerce",
  "model"="dimension"
)
AS
SELECT
  p.product_id AS product_key,
  p.product_id,
  p.sku,
  p.product_name,
  p.brand,
  p.category,
  p.subcategory,
  p.standard_cost,
  p.list_price,
  p.is_active,
  p.created_at AS product_created_at,
  p.updated_at AS product_updated_at,
  p.silver_last_updated AS dim_last_updated
FROM silver.mlv_product p
