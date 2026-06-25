COMMENT "Gold daily sales fact for executive KPI reporting"
TBLPROPERTIES (
  "data_zone"="gold",
  "domain"="ecommerce",
  "model"="fact"
)
AS
SELECT
  fo.order_date_key,
  COUNT(DISTINCT fo.order_id) AS orders_count,
  COUNT(DISTINCT fo.customer_key) AS customers_count,
  SUM(fo.total_items) AS items_sold,
  SUM(fo.gross_revenue) AS gross_revenue,
  SUM(fo.net_revenue) AS net_revenue,
  SUM(fo.margin_amount) AS margin_amount,
  AVG(fo.net_revenue) AS avg_order_value,
  SUM(CASE WHEN fo.is_delivered = 1 THEN 1 ELSE 0 END) AS delivered_orders_count,
  CURRENT_TIMESTAMP() AS fact_last_updated
FROM gold.fact_order fo
GROUP BY fo.order_date_key
