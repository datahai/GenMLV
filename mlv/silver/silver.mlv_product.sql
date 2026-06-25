COMMENT "Ecommerce silver product conformance using bronze raw products"
TBLPROPERTIES (
  "data_zone"="silver",
  "domain"="ecommerce",
  "entity"="product"
)
AS
SELECT
  CAST(p.product_id AS BIGINT) AS product_id,
  CAST(p.sku AS STRING) AS sku,
  INITCAP(TRIM(CAST(p.product_name AS STRING))) AS product_name,
  INITCAP(TRIM(CAST(p.brand AS STRING))) AS brand,
  INITCAP(TRIM(CAST(p.category AS STRING))) AS category,
  INITCAP(TRIM(CAST(p.subcategory AS STRING))) AS subcategory,
  CAST(p.standard_cost AS DECIMAL(18, 2)) AS standard_cost,
  CAST(p.list_price AS DECIMAL(18, 2)) AS list_price,
  CASE
    WHEN CAST(p.is_active AS STRING) IN ('1', 'true', 'TRUE', 'yes', 'YES') THEN 1
    ELSE 0
  END AS is_active,
  CAST(p.created_at AS TIMESTAMP) AS created_at,
  CAST(p.updated_at AS TIMESTAMP) AS updated_at,
  CURRENT_TIMESTAMP() AS silver_last_updated
FROM bronze.raw_products p
