## Welcome to the Skyward Database Connection Project

This repository is maintained as a backend connection between the Skyward SIS cloud storage and PPSD's local ODS.

### Index:
1. ETL Func Python Library -- *Houses all functions used in the main run script for mirroring tables across databases*

2. Main script -- *Coded run command that establishes database connections and moves data*

3. Config Files directory -- *Stores database connection and table list configuration data which main script uses to organize itself. Do not edit!*

4. Table Lists Directory -- *Partitions lists of Skyward DB tables based on schema group, and controls which if any fields are passed over when mirroring tables accross databases due to data type errors etc*

### Current schemas available:
The following schemas can be referred to by name when triggering a manual run of the database mirroring script.
- student-ew -- All tables associated with entry withdrawal database
- courses -- Student/teacher course rostering history
- demographics -- Student service classifications and demographic data

### Automated Scheduling:
Currently, table groups for student entry/withdrawal and demographic histories are set to auto load daily.

### Manual Runs:
If a manual load of tables must be done, any user can trigger the load by opening an instance of PowerShell, and entering the following command strings:

1. `cd 'C:/Reports/Script Files/Skyward_DB_ETLs/'`
2. `python main.py (table list variable)`

Where **table list variable** refers to the schema name to be loaded.
