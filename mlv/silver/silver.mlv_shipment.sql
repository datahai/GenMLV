COMMENT "Ecommerce silver shipment conformance using bronze raw shipments"
TBLPROPERTIES (
  "data_zone"="silver",
  "domain"="ecommerce",
  "entity"="shipment"
)
AS
SELECT
  CAST(s.shipment_id AS BIGINT) AS shipment_id,
  CAST(s.order_id AS BIGINT) AS order_id,
  UPPER(TRIM(CAST(s.carrier AS STRING))) AS carrier,
  UPPER(TRIM(CAST(s.service_level AS STRING))) AS service_level,
  UPPER(TRIM(CAST(s.shipment_status AS STRING))) AS shipment_status,
  CAST(s.shipped_ts AS TIMESTAMP) AS shipped_ts,
  CAST(s.delivered_ts AS TIMESTAMP) AS delivered_ts,
  CAST(s.shipping_cost AS DECIMAL(18, 2)) AS shipping_cost,
  CAST(s.created_at AS TIMESTAMP) AS created_at,
  CURRENT_TIMESTAMP() AS silver_last_updated
FROM bronze.raw_shipments s
