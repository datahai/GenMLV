# GenMLV
Project to manage MLVs in Microsoft Fabric Lakehouse

- Keeps your Fabric Lakehouse MVLs in sync with .sql files in a Lakehouse Files directory.
- Automatically creates or replaces Materialized Lake Views based on the .sql files.
- Cleans up any MLVs that no longer have a corresponding .sql file.
- Maintains a metadata file to track changes and avoid unnecessary recreation.

---
## Usage Video
There's a video here showing the usage of the GenMLV process: [YouTube](https://www.youtube.com/watch?v=Vanel4tge9o)

---

🔧 1. Setup and Initialization

- sql_root_path: Root directory containing subfolders with .sql files.
- metadata_file_path: Path to a JSON file that stores metadata about the .sql files (e.g., last modified time).

📁 2. Load or Initialize Metadata

If the metadata file doesn't exist:
- Create the directory (if needed).
- Create an empty JSON file to store metadata.

If it exists:
- Try to load the JSON content.
  
📄 3. Discover .sql Files
- Traverse all subdirectories under sql_root_path.
- For each .sql file found:
    - Record its full path.
    - Get its last modified timestamp.
    - Store this info in a dictionary sql_files keyed by the table name (filename without .sql).
- Build a dependency graph from SQL references (FROM/JOIN) so deployment runs in dependency order, regardless of folder placement.

📋 4. Get Existing Materialized Lake Views (MLVs)
- Use Spark SQL to list all existing MLVs.
- Store their names in a set called existing_mlvs.
  
🧹 5. Drop Obsolete MLVs
- Identify MLVs that exist in Spark but no longer have a corresponding .sql file.
- Drop each of these MLVs using DROP MATERIALIZED LAKE VIEW.
- Remove their entries from the metadata.
  
🔄 6. Create or Replace MLVs
- For each .sql file:
    - Compare its last modified time with the timestamp in the metadata.
    - If the file is newer:
        - Read the SQL SELECT statement from the file.
        - Create a new MLV or Update an existing MLV using CREATE OR REPLACE MATERIALIZED LAKE VIEW.
        - Schemas will be created automatically based on file names
        - Update the metadata with the new timestamp.
        
💾 7. Save Updated Metadata
- Write the updated metadata dictionary back to the JSON file.

---
## Expanded Ecommerce Example (Bronze -> Silver -> Gold)

The repository now includes a full ecommerce example under `mlv/` that demonstrates:

- Raw ingestion tables in `bronze` (append-only operational data)
- Conformed `silver` MLVs for cleaned and typed entities
- Dimensional model `gold` MLVs (dimensions and facts)

### Bronze Source Tables Used

The SQL examples assume these raw tables exist:

- `bronze.raw_customers`
- `bronze.raw_products`
- `bronze.raw_orders`
- `bronze.raw_order_items`
- `bronze.raw_payments`
- `bronze.raw_shipments`

### Silver MLVs

Silver examples are in `mlv/01/`:

- `silver.mlv_customer.sql`
- `silver.mlv_product.sql`
- `silver.mlv_order.sql`
- `silver.mlv_order_item.sql`
- `silver.mlv_payment.sql`
- `silver.mlv_shipment.sql`

These MLVs standardize datatypes, normalize text values, and expose reusable business fields like order status, payment status, shipping status, and lifecycle timestamps.

### Gold Dimensional MLVs

Gold examples are in `mlv/02/`:

- `gold.dim_customer.sql`
- `gold.dim_product.sql`
- `gold.dim_date.sql`
- `gold.fact_order_line.sql`
- `gold.fact_order.sql`
- `gold.fact_daily_sales.sql`

These build a star-schema style semantic layer suitable for BI reporting:

- Dimensions: customer, product, date
- Facts: order-line level, order-level, and daily sales rollup

### Dependency-Driven Deployment

You can place the files in any folder structure under `mlv/`. GenMLV resolves dependencies from SQL references and deploys in the required order.
