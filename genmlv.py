
import os
import json
from datetime import datetime

def load_or_initialize_metadata(metadata_file_path):
    if not os.path.exists(metadata_file_path):
        os.makedirs(os.path.dirname(metadata_file_path), exist_ok=True)
        with open(metadata_file_path, 'w') as meta_file:
            json.dump({}, meta_file)
        return {}
    try:
        with open(metadata_file_path, 'r') as meta_file:
            content = meta_file.read().strip()
            return json.loads(content) if content else {}
    except json.JSONDecodeError:
        print("⚠️ Metadata file is corrupted. Initializing new metadata.")
        return {}

def collect_sql_files(sql_root_path):
    sql_files = []
    required_schemas = set()

    for root, dirs, files in os.walk(sql_root_path):
        # Sort directories to ensure correct traversal order (numeric-aware)
        dirs.sort(key=lambda d: int(d) if d.isdigit() else d)

        # Sort files within each folder
        for f in sorted(files):
            if f.endswith(".sql"):
                full_path = os.path.join(root, f)
                modified_time = os.path.getmtime(full_path)
                modified_dt = datetime.fromtimestamp(modified_time)
                base_name = f[:-4]
                schema, table_name = base_name.split('.', 1) if '.' in base_name else ("default", base_name)
                required_schemas.add(schema)
                sql_files.append({
                    "schema": schema,
                    "table_name": table_name,
                    "path": full_path,
                    "timestamp": modified_time,
                    "datetime": modified_dt.strftime("%Y-%m-%d %H:%M:%S")
                })

    return sql_files, required_schemas

def ensure_schemas_exist(required_schemas, dry_run=False):
    existing_schemas = set(row.namespace.split('.')[-1] for row in spark.sql("SHOW SCHEMAS").collect())
    for schema in required_schemas - existing_schemas:
        print(f"{'Would create' if dry_run else 'Creating'} missing schema: {schema}")
        if not dry_run:
            try:
                spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                print(f"✅ Schema '{schema}' created.")
            except Exception as e:
                print(f"❌ Failed to create schema '{schema}': {e}")

def drop_obsolete_mlvs(sql_files, mlv_metadata, dry_run=False):
    all_schemas = set(row.namespace.split('.')[-1] for row in spark.sql("SHOW SCHEMAS").collect())
    existing_mlvs = set()

    for schema in all_schemas:
        try:
            result = spark.sql(f"SHOW MATERIALIZED LAKE VIEWS IN {schema}").collect()
            for row in result:
                existing_mlvs.add((schema, row.name))
        except Exception as e:
            print(f"⚠️ Could not list MLVs in schema '{schema}': {e}")

    # Convert sql_files list to set of tuples for comparison
    sql_file_keys = {(item['schema'], item['table_name']) for item in sql_files}
    mlvs_to_drop = existing_mlvs - sql_file_keys

    for schema, mlv in mlvs_to_drop:
        print(f"{'Would drop' if dry_run else 'Dropping'} obsolete MLV: {schema}.{mlv}")
        if not dry_run:
            try:
                spark.sql(f"DROP MATERIALIZED LAKE VIEW IF EXISTS {schema}.{mlv}")
                print(f"✅ Dropped obsolete MLV: {schema}.{mlv}")
                mlv_metadata.pop(f"{schema}.{mlv}", None)
            except Exception as e:
                print(f"❌ Failed to drop MLV '{schema}.{mlv}': {e}")

    return existing_mlvs

def create_or_update_mlvs(sql_files, existing_mlvs, mlv_metadata, dry_run=False):
    for file_info in sql_files:
        schema = file_info["schema"]
        table_name = file_info["table_name"]
        file_path = file_info["path"]
        modified_datetime = datetime.strptime(file_info["datetime"], "%Y-%m-%d %H:%M:%S")
        metadata_key = f"{schema}.{table_name}"
        last_processed_str = mlv_metadata.get(metadata_key, {}).get("datetime", "1970-01-01 00:00:00")
        last_processed_datetime = datetime.strptime(last_processed_str, "%Y-%m-%d %H:%M:%S")

        # Note: ensure '>' is a literal greater-than (avoid pasted HTML entities like &gt;)
        if modified_datetime > last_processed_datetime:
            with open(file_path, 'r') as file:
                select_statement = file.read().strip()

            print(f"{'Would create/replace' if dry_run else 'Creating or replacing'} MLV: {schema}.{table_name}")
            if not dry_run:
                try:
                    # No DROP anymore; rely on CREATE OR REPLACE
                    create_sql = (
                        f"CREATE OR REPLACE MATERIALIZED LAKE VIEW "
                        f"{schema}.{table_name} {select_statement}"
                    )
                    spark.sql(create_sql)
                    print(f"✅ MLV '{schema}.{table_name}' created or replaced successfully.")

                    mlv_metadata[metadata_key] = {
                        "timestamp": file_info["timestamp"],
                        "datetime": file_info["datetime"]
                    }
                except Exception as e:
                    print(f"❌ Failed to create/replace MLV '{schema}.{table_name}': {e}")

def save_metadata(metadata_file_path, mlv_metadata):
    with open(metadata_file_path, 'w') as meta_file:
        json.dump(mlv_metadata, meta_file, indent=2)

# === Main Execution ===
def main(dry_run=False):
    sql_root_path = "/lakehouse/default/Files/mlv"
    metadata_file_path = os.path.join(sql_root_path, "mlv_metadata.json")

    mlv_metadata = load_or_initialize_metadata(metadata_file_path)

    sql_files, required_schemas = collect_sql_files(sql_root_path)

    ensure_schemas_exist(required_schemas, dry_run)

    existing_mlvs = drop_obsolete_mlvs(sql_files, mlv_metadata, dry_run)

    create_or_update_mlvs(sql_files, existing_mlvs, mlv_metadata, dry_run)

    if not dry_run:
        save_metadata(metadata_file_path, mlv_metadata)
    else:
        print("📝 Dry run mode: Metadata not saved.")

##################################################################################################
# Run
main(dry_run=False)
