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
    sql_files = {}
    required_schemas = set()
    for subdir in sorted(os.listdir(sql_root_path)):
        subdir_path = os.path.join(sql_root_path, subdir)
        if os.path.isdir(subdir_path):
            for f in sorted(os.listdir(subdir_path)):
                if f.endswith(".sql"):
                    full_path = os.path.join(subdir_path, f)
                    modified_time = os.path.getmtime(full_path)
                    modified_dt = datetime.fromtimestamp(modified_time)
                    base_name = f[:-4]
                    schema, table_name = base_name.split('.', 1) if '.' in base_name else ("default", base_name)
                    required_schemas.add(schema)
                    sql_files[(schema, table_name)] = {
                        "path": full_path,
                        "timestamp": modified_time,
                        "datetime": modified_dt.strftime("%Y-%m-%d %H:%M:%S")
                    }
    return sql_files, required_schemas

def ensure_schemas_exist(required_schemas):
    existing_schemas = set(row.namespace.split('.')[-1] for row in spark.sql("SHOW SCHEMAS").collect())
    for schema in required_schemas - existing_schemas:
        try:
            print(f"Creating missing schema: {schema}")
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            print(f"✅ Schema '{schema}' created.")
        except Exception as e:
            print(f"❌ Failed to create schema '{schema}': {e}")

def drop_obsolete_mlvs(sql_files, mlv_metadata):
    # Always get all schemas from the lakehouse
    all_schemas = set(row.namespace.split('.')[-1] for row in spark.sql("SHOW SCHEMAS").collect())
    existing_mlvs = set()

    for schema in all_schemas:
        try:
            result = spark.sql(f"SHOW MATERIALIZED LAKE VIEWS IN {schema}").collect()
            for row in result:
                existing_mlvs.add((schema, row.name))
        except Exception as e:
            print(f"⚠️ Could not list MLVs in schema '{schema}': {e}")

    mlvs_to_drop = existing_mlvs - set(sql_files.keys())

    for schema, mlv in mlvs_to_drop:
        try:
            print(f"Dropping obsolete MLV: {schema}.{mlv}")
            spark.sql(f"DROP MATERIALIZED LAKE VIEW IF EXISTS {schema}.{mlv}")
            print(f"✅ Dropped obsolete MLV: {schema}.{mlv}")
            mlv_metadata.pop(f"{schema}.{mlv}", None)
        except Exception as e:
            print(f"❌ Failed to drop MLV '{schema}.{mlv}': {e}")

    return existing_mlvs


def create_or_update_mlvs(sql_files, existing_mlvs, mlv_metadata):
    for (schema, table_name), file_info in sql_files.items():
        file_path = file_info["path"]
        modified_datetime = datetime.strptime(file_info["datetime"], "%Y-%m-%d %H:%M:%S")
        metadata_key = f"{schema}.{table_name}"
        last_processed_str = mlv_metadata.get(metadata_key, {}).get("datetime", "1970-01-01 00:00:00")
        last_processed_datetime = datetime.strptime(last_processed_str, "%Y-%m-%d %H:%M:%S")

        if modified_datetime > last_processed_datetime:
            with open(file_path, 'r') as file:
                select_statement = file.read().strip()
            try:
                if (schema, table_name) in existing_mlvs:
                    print(f"Dropping existing MLV before recreation: {schema}.{table_name}")
                    spark.sql(f"DROP MATERIALIZED LAKE VIEW IF EXISTS {schema}.{table_name}")
                create_sql = f"CREATE MATERIALIZED LAKE VIEW {schema}.{table_name} {select_statement}"
                print(f"Executing SQL to create MLV: {schema}.{table_name}")
                spark.sql(create_sql)
                print(f"✅ MLV '{schema}.{table_name}' created successfully.")
                mlv_metadata[metadata_key] = {
                    "timestamp": file_info["timestamp"],
                    "datetime": file_info["datetime"]
                }
            except Exception as e:
                print(f"❌ Failed to create MLV '{schema}.{table_name}': {e}")

def save_metadata(metadata_file_path, mlv_metadata):
    with open(metadata_file_path, 'w') as meta_file:
        json.dump(mlv_metadata, meta_file, indent=2)

# === Main Execution ===
def main():
    sql_root_path = "/lakehouse/default/Files/mlv"
    metadata_file_path = os.path.join(sql_root_path, "mlv_metadata.json")

    mlv_metadata = load_or_initialize_metadata(metadata_file_path)

    sql_files, required_schemas = collect_sql_files(sql_root_path)

    ensure_schemas_exist(required_schemas)

    existing_mlvs = drop_obsolete_mlvs(sql_files, mlv_metadata)

    create_or_update_mlvs(sql_files, existing_mlvs, mlv_metadata)

    save_metadata(metadata_file_path, mlv_metadata)

main()
