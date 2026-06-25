COMMENT "Ecommerce silver payment conformance using bronze raw payments"
TBLPROPERTIES (
  "data_zone"="silver",
  "domain"="ecommerce",
  "entity"="payment"
)
AS
SELECT
  CAST(p.payment_id AS BIGINT) AS payment_id,
  CAST(p.order_id AS BIGINT) AS order_id,
  UPPER(TRIM(CAST(p.payment_method AS STRING))) AS payment_method,
  UPPER(TRIM(CAST(p.payment_status AS STRING))) AS payment_status,
  CAST(p.amount AS DECIMAL(18, 2)) AS amount,
  CAST(p.payment_ts AS TIMESTAMP) AS payment_ts,
  CAST(p.created_at AS TIMESTAMP) AS created_at,
  CURRENT_TIMESTAMP() AS silver_last_updated
FROM bronze.raw_payments p
