COMMENT "Ecommerce silver order item conformance using bronze raw order items"
TBLPROPERTIES (
  "data_zone"="silver",
  "domain"="ecommerce",
  "entity"="order_item"
)
AS
SELECT
  CAST(oi.order_id AS BIGINT) AS order_id,
  CAST(oi.order_line_id AS BIGINT) AS order_line_id,
  CAST(oi.product_id AS BIGINT) AS product_id,
  CAST(oi.quantity AS INT) AS quantity,
  CAST(oi.unit_price AS DECIMAL(18, 2)) AS unit_price,
  CAST(oi.unit_discount AS DECIMAL(18, 2)) AS unit_discount,
  CAST((oi.quantity * (oi.unit_price - COALESCE(oi.unit_discount, 0))) AS DECIMAL(18, 2)) AS line_net_amount,
  CAST((oi.quantity * oi.unit_price) AS DECIMAL(18, 2)) AS line_gross_amount,
  CAST(oi.created_at AS TIMESTAMP) AS created_at,
  CURRENT_TIMESTAMP() AS silver_last_updated
FROM bronze.raw_order_items oi
