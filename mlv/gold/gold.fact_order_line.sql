COMMENT "Gold order line fact with customer, product, payment, and shipment signals"
TBLPROPERTIES (
  "data_zone"="gold",
  "domain"="ecommerce",
  "model"="fact"
)
AS
WITH payment_agg AS (
  SELECT
    p.order_id,
    SUM(CASE WHEN p.payment_status = 'SUCCEEDED' THEN p.amount ELSE 0 END) AS paid_amount,
    MAX(CASE WHEN p.payment_status = 'SUCCEEDED' THEN p.payment_ts END) AS latest_paid_ts,
    MAX(CASE WHEN p.payment_status = 'FAILED' THEN 1 ELSE 0 END) AS has_failed_payment
  FROM silver.mlv_payment p
  GROUP BY p.order_id
),
shipment_agg AS (
  SELECT
    s.order_id,
    MIN(s.shipped_ts) AS first_shipped_ts,
    MAX(s.delivered_ts) AS latest_delivered_ts,
    SUM(COALESCE(s.shipping_cost, 0)) AS total_shipping_cost,
    MAX(CASE WHEN s.shipment_status = 'DELIVERED' THEN 1 ELSE 0 END) AS is_delivered
  FROM silver.mlv_shipment s
  GROUP BY s.order_id
)
SELECT
  o.order_id,
  oi.order_line_id,
  CAST(DATE_FORMAT(o.order_ts, 'yyyyMMdd') AS INT) AS order_date_key,
  o.customer_id AS customer_key,
  oi.product_id AS product_key,
  o.sales_channel,
  o.currency_code,
  o.order_status,
  oi.quantity,
  oi.unit_price,
  oi.unit_discount,
  oi.line_gross_amount,
  oi.line_net_amount,
  CAST((oi.line_net_amount - (oi.quantity * COALESCE(dp.standard_cost, 0))) AS DECIMAL(18, 2)) AS line_margin_amount,
  COALESCE(pa.paid_amount, 0) AS order_paid_amount,
  COALESCE(pa.has_failed_payment, 0) AS has_failed_payment,
  pa.latest_paid_ts,
  sa.first_shipped_ts,
  sa.latest_delivered_ts,
  COALESCE(sa.total_shipping_cost, 0) AS order_shipping_cost,
  COALESCE(sa.is_delivered, 0) AS is_delivered,
  CURRENT_TIMESTAMP() AS fact_last_updated
FROM silver.mlv_order o
INNER JOIN silver.mlv_order_item oi
  ON o.order_id = oi.order_id
LEFT JOIN gold.dim_product dp
  ON oi.product_id = dp.product_key
LEFT JOIN payment_agg pa
  ON o.order_id = pa.order_id
LEFT JOIN shipment_agg sa
  ON o.order_id = sa.order_id
