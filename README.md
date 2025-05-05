## Prerequisites
* Clone this repo, and the utils repo.
* Create a venv before installing packages
* If source files are in synapse, login and set up a personal access token. Reference the docs.
  - [Synapse Authentication Guide](https://python-docs.synapse.org/en/stable/tutorials/authentication/)
  - [Synapse REST Table Examples](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/web/controller/TableExamples.html)
  - Add the PAT to .synapseConfig 
* Set-up .bash_profile. See template below
* db connection
  - see local postgres connection notes below to create a new db
  - db connection will depend on organization and project.
* Set up a profiles.yml
* Data file requirements.
- `src/{study}_study.yaml`, `ftd_study.yaml`and data files in `data/{study}`

## Set vars in .bash_profile(suggested)
```
export PYTHONPATH=~{path_to_utils}/dbt-utils/src:$PYTHONPATH
export PG_USER={pg_username} # could instead use ~/.pgpass
export PG_PASS={pg_password} # could instead use ~/.pgpass
```

## Requirements
```bash
pip install -r ../requirements.txt
```

## Setup
### Creating a New Postgres DB
The local PostgreSQL database will store tables from the pipeline run. Each study requires a separate DB, but you can reuse the same role/user.

### 1. Log into PostgreSQL as Admin
```bash
sudo -u postgres psql
```
Exit with `\q`.

### 2. Create a Role
```sql
CREATE ROLE {my_role} LOGIN CREATEDB PASSWORD '{password}';
-- Example:
-- CREATE ROLE dev_role LOGIN CREATEDB PASSWORD 'my_password';
```
Drop and retry if needed:
```sql
DROP ROLE {my_role};
SELECT rolname FROM pg_roles WHERE rolcanlogin;
```

### 3. Create a Database
```sql
CREATE DATABASE {db};
```

### 4. Create a User
```sql
CREATE USER {my_user} WITH PASSWORD '{password}';
GRANT {my_role} TO {my_user};
GRANT ALL PRIVILEGES ON DATABASE {db} TO {my_user};
ALTER ROLE {my_user} WITH SUPERUSER; -- Needed for COPY
```

### 5. Connect as the New User
```bash
sudo systemctl restart postgresql
psql -U {my_user} -d {db} -h {host} -p {port}
# Example: psql -U my_user -d default_db -h localhost -p 5432
```

### 6. Save Your Password for Convenience
```bash

chmod 600 ~/.pgpass
```

### Helpful Queries
```sql
SELECT current_database(), current_user;
SELECT schemaname, tablename FROM pg_tables;
SELECT schemaname, tablename FROM pg_tables
WHERE schemaname NOT LIKE 'pg%'
AND schemaname NOT LIKE 'information%'
AND tablename NOT LIKE 'my_first_dbt_model'
ORDER BY schemaname;

-- Drop tables/schemas
DROP TABLE pg_tables.{your_table_name};
DROP SCHEMA dbo_aecom_data CASCADE;

-- Inspect table data
SELECT * FROM schemaname.tablename LIMIT 50;
```

## Project Workflow
- Run everything from inside the "ftd" project directory.

### Study YAML Creation
- Ensure table details are accurate: use filenames for CSVs or Synapse IDs.
- Replace all `m00m00` placeholders with your study ID.
- Check `profiles.yml` in the examples dir for the expected file format.


### Run Order
1. `dbt clean && dbt deps`
2. Log into your database
3. Import source data (if needed)
```bash
./scripts/import_data.py -s {study_id}
```
- Delete tables if re-importing due to errors
```sql
DROP TABLE schema_name.table_name;
```
4. Generate model docs
```bash
./scripts/generate_model_docs.py
```
- Replace all `m00m00` placeholders manually
- Study-specific updates may be needed
- Give permissions to new scripts if necessary

```bash
chmod +x scripts/process_new_study.py
```

### Transformation Workflow
- Test models step-by-step with `dbt run`
- Add working models to `packages.yml`
- Use `scripts/examples/ex_run_commands.sh` for command templates
- Run `dbt clean && dbt deps` frequently

### Debugging Tips
- If getting new errors related to doc files, suggested fix is to delete the file and regenerate it.
- Check the root `dbt_project.yml` import paths, if a module is unknown by dbt


## Final Goal
Generate a complete dbt pipeline for the study including:
- Study-specific configs
- Model transformations
- Usable repository with all requirements

