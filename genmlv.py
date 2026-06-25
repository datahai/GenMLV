
import os
import json
import re
from collections import deque
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

def extract_sql_references(sql_text):
    search_text = get_dependency_search_section(sql_text)
    sanitized = strip_sql_noise(search_text)
    cte_aliases = extract_cte_aliases(sanitized)
    # Capture references after FROM/JOIN with optional schema qualifier.
    regex = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][\w$]*(?:\.[a-zA-Z_][\w$]*)?)", re.IGNORECASE)
    refs = []
    for ref in regex.findall(sanitized):
        if ref.lower() not in cte_aliases:
            refs.append(ref)
    return refs

def get_dependency_search_section(sql_text):
    # MLV files usually have options followed by AS SELECT. Search dependencies
    # in the SQL body to avoid false positives from COMMENT/TBLPROPERTIES text.
    match = re.search(r"\bAS\b", sql_text, flags=re.IGNORECASE)
    if not match:
        return sql_text
    return sql_text[match.end():]

def strip_sql_noise(sql_text):
    cleaned = re.sub(r"/\*[\s\S]*?\*/", " ", sql_text)
    cleaned = re.sub(r"--.*$", " ", cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r"'(?:''|[^'])*'", " ", cleaned)
    cleaned = re.sub(r'"(?:""|[^"])*"', " ", cleaned)
    return cleaned

def extract_cte_aliases(sql_text):
    cte_regex = re.compile(r"(?:\bwith\b|,)\s*([a-zA-Z_][\w$]*)\s+as\s*\(", re.IGNORECASE)
    return {match.lower() for match in cte_regex.findall(sql_text)}

def normalize_reference(raw_ref):
    cleaned = raw_ref.strip().replace('`', '').replace('[', '').replace(']', '').replace('"', '')
    if not cleaned or cleaned.startswith('('):
        return None
    return cleaned

def resolve_reference(reference, all_keys_lower, table_to_key):
    lowered = reference.lower()
    if '.' in reference:
        return reference if lowered in all_keys_lower else None
    return table_to_key.get(lowered)

def order_sql_files_by_dependency(sql_files):
    by_key = {f"{item['schema']}.{item['table_name']}": item for item in sql_files}
    all_keys = sorted(by_key.keys())
    all_keys_lower = {key.lower() for key in all_keys}
    table_to_key = {item['table_name'].lower(): f"{item['schema']}.{item['table_name']}" for item in sql_files}

    outgoing = {key: set() for key in all_keys}
    indegree = {key: 0 for key in all_keys}

    for key in all_keys:
        file_info = by_key[key]
        with open(file_info['path'], 'r') as sql_file:
            sql_text = sql_file.read()

        for raw_ref in extract_sql_references(sql_text):
            normalized = normalize_reference(raw_ref)
            if not normalized:
                continue

            dependency_key = resolve_reference(normalized, all_keys_lower, table_to_key)
            if not dependency_key or dependency_key == key:
                continue

            if key not in outgoing[dependency_key]:
                outgoing[dependency_key].add(key)
                indegree[key] += 1

    ready = deque(sorted([key for key in all_keys if indegree[key] == 0]))
    ordered_keys = []

    while ready:
        current = ready.popleft()
        ordered_keys.append(current)

        for dependent in sorted(outgoing[current]):
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                ready.append(dependent)

    if len(ordered_keys) != len(all_keys):
        remaining = sorted([key for key in all_keys if key not in set(ordered_keys)])
        print("⚠️ Detected dependency cycle or unresolved ordering among:", ", ".join(remaining))
        print("⚠️ Falling back to alphabetical order for the remaining items.")
        ordered_keys.extend(remaining)

    return [by_key[key] for key in ordered_keys]

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
    ordered_sql_files = order_sql_files_by_dependency(sql_files)

    for file_info in ordered_sql_files:
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
