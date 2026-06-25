COMMENT "Ecommerce silver order conformance using bronze raw orders"
TBLPROPERTIES (
  "data_zone"="silver",
  "domain"="ecommerce",
  "entity"="order"
)
AS
SELECT
  CAST(o.order_id AS BIGINT) AS order_id,
  CAST(o.customer_id AS BIGINT) AS customer_id,
  UPPER(TRIM(CAST(o.order_status AS STRING))) AS order_status,
  UPPER(TRIM(CAST(o.sales_channel AS STRING))) AS sales_channel,
  UPPER(TRIM(CAST(o.currency_code AS STRING))) AS currency_code,
  CAST(o.order_ts AS TIMESTAMP) AS order_ts,
  CAST(o.ship_by_ts AS TIMESTAMP) AS ship_by_ts,
  CAST(o.cancelled_ts AS TIMESTAMP) AS cancelled_ts,
  CAST(o.subtotal_amount AS DECIMAL(18, 2)) AS subtotal_amount,
  CAST(o.discount_amount AS DECIMAL(18, 2)) AS discount_amount,
  CAST(o.shipping_amount AS DECIMAL(18, 2)) AS shipping_amount,
  CAST(o.tax_amount AS DECIMAL(18, 2)) AS tax_amount,
  CAST(o.order_total_amount AS DECIMAL(18, 2)) AS order_total_amount,
  CAST(o.created_at AS TIMESTAMP) AS created_at,
  CAST(o.updated_at AS TIMESTAMP) AS updated_at,
  CURRENT_TIMESTAMP() AS silver_last_updated
FROM bronze.raw_orders o
