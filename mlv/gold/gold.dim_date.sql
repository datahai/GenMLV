COMMENT "Gold date dimension generated using ecommerce order timestamps"
TBLPROPERTIES (
  "data_zone"="gold",
  "domain"="ecommerce",
  "model"="dimension"
)
AS
SELECT DISTINCT
  CAST(DATE_FORMAT(o.order_ts, 'yyyyMMdd') AS INT) AS date_key,
  CAST(o.order_ts AS DATE) AS full_date,
  YEAR(o.order_ts) AS year_num,
  QUARTER(o.order_ts) AS quarter_num,
  MONTH(o.order_ts) AS month_num,
  DATE_FORMAT(o.order_ts, 'MMMM') AS month_name,
  WEEKOFYEAR(o.order_ts) AS week_num,
  DAY(o.order_ts) AS day_of_month,
  DAYOFWEEK(o.order_ts) AS day_of_week,
  DATE_FORMAT(o.order_ts, 'EEEE') AS day_name,
  CASE WHEN DAYOFWEEK(o.order_ts) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
  CURRENT_TIMESTAMP() AS dim_last_updated
FROM silver.mlv_order o
WHERE o.order_ts IS NOT NULL
