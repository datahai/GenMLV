COMMENT "Ecommerce silver customer conformance using bronze raw customers"
TBLPROPERTIES (
  "data_zone"="silver",
  "domain"="ecommerce",
  "entity"="customer"
)
AS
SELECT
  CAST(c.customer_id AS BIGINT) AS customer_id,
  LOWER(TRIM(CAST(c.email AS STRING))) AS email,
  INITCAP(TRIM(CAST(c.first_name AS STRING))) AS first_name,
  INITCAP(TRIM(CAST(c.last_name AS STRING))) AS last_name,
  CONCAT(INITCAP(TRIM(CAST(c.first_name AS STRING))), ' ', INITCAP(TRIM(CAST(c.last_name AS STRING)))) AS full_name,
  INITCAP(TRIM(CAST(c.gender AS STRING))) AS gender,
  CAST(c.birth_date AS DATE) AS birth_date,
  INITCAP(TRIM(CAST(c.phone AS STRING))) AS phone,
  INITCAP(TRIM(CAST(c.city AS STRING))) AS city,
  UPPER(TRIM(CAST(c.state_province AS STRING))) AS state_province,
  UPPER(TRIM(CAST(c.country AS STRING))) AS country,
  CASE
    WHEN CAST(c.marketing_opt_in AS STRING) IN ('1', 'true', 'TRUE', 'yes', 'YES') THEN 1
    ELSE 0
  END AS marketing_opt_in,
  CAST(c.created_at AS TIMESTAMP) AS created_at,
  CAST(c.updated_at AS TIMESTAMP) AS updated_at,
  CURRENT_TIMESTAMP() AS silver_last_updated
FROM bronze.raw_customers c
