# Deploy GenMLV
To deploy and use GenMLV:

- Create a Notebook and add the code in the genmlv.py file
- Create a Lakehouse with schema enabled (preview only)
- In the Lakehouse Files section, create a folder called mlv
- In this mlv folder you can create sub-folders e.g. 01, 02, 03 etc.  This is because we can add .sql files to directories to run in sequence.  E.G any .sql files in 01 need to be run before files in 02 folder etc.  Unfortunately at this time MLVs don't have any "ref" like functionality (Dbt etc)

To write the SQL transformations:
- Create a file with the schema and name of the MLV you want to create e.g. "silver.mlv_product"
- In this file, add the SQL transformation required to create the MLV.  You must omit the CREATE MATERIALIZED LAKE VIEW as this is handled in the process.  The format of the file must be:

```
[( 
    CONSTRAINT constraint_name1 CHECK (condition expression1)[ON MISMATCH DROP | FAIL],  
    CONSTRAINT constraint_name2 CHECK (condition expression2)[ON MISMATCH DROP | FAIL] 
)] 
[PARTITIONED BY (col1, col2, ... )] 
[COMMENT “description or comment”] 
[TBLPROPERTIES (“key1”=”val1”, “key2”=”val2”, ... )] 
AS select_statement
```
E.G (basic example...I'm not doing anything complex here)

```
COMMENT "Generated by GenMLV"
TBLPROPERTIES ("data_zone"="silver")
AS
SELECT
    *,
    now() as SilverLastUpdated
FROM Product
```

- Upload the .sql file to the mvl/01 directory
<img width="648" height="352" alt="image" src="https://github.com/user-attachments/assets/1ee1f92a-066e-420d-bf52-5eb882dbf893" />


- You can now run the Notebook cell with the genmlv logic in.



