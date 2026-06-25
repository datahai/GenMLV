COMMENT "Gold customer dimension built using silver customer MLV"
TBLPROPERTIES (
  "data_zone"="gold",
  "domain"="ecommerce",
  "model"="dimension"
)
AS
SELECT
  c.customer_id AS customer_key,
  c.customer_id,
  c.email,
  c.first_name,
  c.last_name,
  c.full_name,
  c.gender,
  c.birth_date,
  c.city,
  c.state_province,
  c.country,
  c.marketing_opt_in,
  c.created_at AS customer_created_at,
  c.updated_at AS customer_updated_at,
  c.silver_last_updated AS dim_last_updated
FROM silver.mlv_customer c
