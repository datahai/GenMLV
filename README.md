# GenMLV
Project to manage MLVs in Microsoft Fabric Lakehouse

- Keeps your Fabric Lakehouse MVLs in sync with .sql files in a Lakehouse Files directory.
- Automatically creates or updates Materialized Lake Views based on the .sql files files.
- Cleans up any MLVs that no longer have a corresponding .sql file.
- Maintains a metadata file to track changes and avoid unnecessary recreation.

🔧 1. Setup and Initialization

- sql_root_path: Root directory containing subfolders with .sql files.
- metadata_file_path: Path to a JSON file that stores metadata about the .sql files (e.g., last modified time).

📁 2. Load or Initialize Metadata

If the metadata file doesn't exist:
- Create the directory (if needed).
- Create an empty JSON file to store metadata.

If it exists:
- Try to load the JSON content.
- If the file is empty or corrupted, initialize an empty metadata dictionary and print a warning.
  
📄 3. Discover .sql Files
- Traverse all subdirectories under sql_root_path.
- For each .sql file found:
    - Record its full path.
    - Get its last modified timestamp.
    - Store this info in a dictionary sql_files keyed by the table name (filename without .sql).

📋 4. Get Existing Materialized Lake Views (MLVs)
- Use Spark SQL to list all existing MLVs.
- Store their names in a set called existing_mlvs.
  
🧹 5. Drop Obsolete MLVs
- Identify MLVs that exist in Spark but no longer have a corresponding .sql file.
- Drop each of these MLVs using DROP MATERIALIZED LAKE VIEW.
- Remove their entries from the metadata.
  
🔄 6. Create or Recreate MLVs
-For each .sql file:
    - Compare its last modified time with the timestamp in the metadata.
    - If the file is newer:
        - Read the SQL SELECT statement from the file.
        - Drop the existing MLV (if it exists).
        - Create a new MLV using CREATE MATERIALIZED LAKE VIEW.
        - Update the metadata with the new timestamp.
        
💾 7. Save Updated Metadata
- Write the updated metadata dictionary back to the JSON file.
