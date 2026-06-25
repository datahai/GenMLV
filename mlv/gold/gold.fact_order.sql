COMMENT "Gold order-level fact aggregated using order line fact"
TBLPROPERTIES (
  "data_zone"="gold",
  "domain"="ecommerce",
  "model"="fact"
)
AS
SELECT
  fol.order_id,
  fol.order_date_key,
  fol.customer_key,
  MAX(fol.sales_channel) AS sales_channel,
  MAX(fol.currency_code) AS currency_code,
  MAX(fol.order_status) AS order_status,
  SUM(fol.quantity) AS total_items,
  COUNT(DISTINCT fol.product_key) AS distinct_products,
  SUM(fol.line_gross_amount) AS gross_revenue,
  SUM(fol.line_net_amount) AS net_revenue,
  SUM(fol.line_margin_amount) AS margin_amount,
  MAX(fol.order_paid_amount) AS order_paid_amount,
  MAX(fol.has_failed_payment) AS has_failed_payment,
  MAX(fol.is_delivered) AS is_delivered,
  MAX(fol.latest_paid_ts) AS latest_paid_ts,
  MAX(fol.first_shipped_ts) AS first_shipped_ts,
  MAX(fol.latest_delivered_ts) AS latest_delivered_ts,
  MAX(fol.order_shipping_cost) AS order_shipping_cost,
  CURRENT_TIMESTAMP() AS fact_last_updated
FROM gold.fact_order_line fol
GROUP BY
  fol.order_id,
  fol.order_date_key,
  fol.customer_key
