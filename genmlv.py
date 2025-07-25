import os
import json
from datetime import datetime

# Root folder containing subdirectories with .sql files
sql_root_path = "/lakehouse/default/Files/mlv"
metadata_file_path = os.path.join(sql_root_path, "mlv_metadata.json")

# Load or create metadata file
mlv_metadata = {}
if not os.path.exists(metadata_file_path):
    os.makedirs(os.path.dirname(metadata_file_path), exist_ok=True)
    with open(metadata_file_path, 'w') as meta_file:
        json.dump(mlv_metadata, meta_file)
else:
    try:
        with open(metadata_file_path, 'r') as meta_file:
            content = meta_file.read().strip()
            if content:
                mlv_metadata = json.loads(content)
            else:
                print("⚠️ Metadata file is empty. Initializing new metadata.")
    except json.JSONDecodeError:
        print("⚠️ Metadata file is corrupted. Initializing new metadata.")

# Collect all .sql files from subdirectories (sorted alphabetically)
sql_files = {}
for subdir in sorted(os.listdir(sql_root_path)):
    subdir_path = os.path.join(sql_root_path, subdir)
    if os.path.isdir(subdir_path):
        for f in sorted(os.listdir(subdir_path)):
            if f.endswith(".sql"):
                full_path = os.path.join(subdir_path, f)
                modified_time = os.path.getmtime(full_path)
                modified_dt = datetime.fromtimestamp(modified_time)
                table_name = f[:-4]  # Remove .sql extension
                sql_files[table_name] = {
                    "path": full_path,
                    "timestamp": modified_time,
                    "datetime": modified_dt.strftime("%Y-%m-%d %H:%M:%S")
                }

# Get list of existing MLVs
existing_mlvs = set(row.name for row in spark.sql("SHOW MATERIALIZED LAKE VIEWS").collect())

# Drop MLVs that no longer have a corresponding .sql file
mlvs_to_drop = existing_mlvs - set(sql_files.keys())
for mlv in mlvs_to_drop:
    try:
        print(f"Dropping obsolete MLV: {mlv}")
        spark.sql(f"DROP MATERIALIZED LAKE VIEW IF EXISTS {mlv}")
        print(f"✅ Dropped obsolete MLV: {mlv}")
        mlv_metadata.pop(mlv, None)
    except Exception as e:
        print(f"❌ Failed to drop MLV '{mlv}': {e}")

# Create or recreate MLVs from .sql files if modified
for table_name, file_info in sql_files.items():
    file_path = file_info["path"]
    modified_datetime = datetime.strptime(file_info["datetime"], "%Y-%m-%d %H:%M:%S")
    last_processed_str = mlv_metadata.get(table_name, {}).get("datetime", "1970-01-01 00:00:00")
    last_processed_datetime = datetime.strptime(last_processed_str, "%Y-%m-%d %H:%M:%S")

    if modified_datetime > last_processed_datetime:
        with open(file_path, 'r') as file:
            select_statement = file.read().strip()

        try:
            if table_name in existing_mlvs:
                print(f"Dropping existing MLV before recreation: {table_name}")
                spark.sql(f"DROP MATERIALIZED LAKE VIEW IF EXISTS {table_name}")

            create_table_sql = f"CREATE MATERIALIZED LAKE VIEW {table_name} {select_statement}"
            print(f"Executing SQL to create MLV: {table_name}")
            spark.sql(create_table_sql)
            print(f"✅ MLV '{table_name}' created successfully.")

            # Update metadata
            mlv_metadata[table_name] = {
                "timestamp": file_info["timestamp"],
                "datetime": file_info["datetime"]
            }

        except Exception as e:
            print(f"❌ Failed to create MLV '{table_name}': {e}")

# Save updated metadata
with open(metadata_file_path, 'w') as meta_file:
    json.dump(mlv_metadata, meta_file, indent=2)
