# Deploy GenMLV
To deploy and use GenMLV:

- Create a Notebook and add the code in the genmlv.py file
- Create a Lakehouse with schema enabled (preview only)
- In the Lakehouse Files section, create a folder called mlv
- In this mlv folder you can create sub-folders e.g. 01, 02, 03 etc.  This is because we can add .sql files to directories to run in sequence.  E.G any .sql files in 01 need tobe run before files in 02 folder etc.
