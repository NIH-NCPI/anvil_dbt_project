# Anvil dbt project
Contains the dbt pipelines for all AnVIL studies.

## Procedure
**REMINDER:** Before any commit! Make sure the the .gitignore files are set up properly before making any commits! They should already be set up and no edits needed, but verify this! 

### 1. Don't Panic 
  -It is a lot of info! Ask a teammate for help, especially for your first env and pipeline setup.

### 2. Setup
Recommended, if running on your local computer(not in Terra), create a venv, and activate it.

1. Install the project requirements with `pip install -r requirements.txt` from the root dir.  This will install the utils package, among other dbt required installations.

2. Set up your `~/.dbt/profiles.yml` if required, define environment variables(secrets) somewhere. Ask team about our secrets handling strategy. Example profiles exist in `/root_examples/profiles/ex_*`. If more than one is required, place them all to the same file. `~/.dbt/profiles.yml`
```bash
# Check if file or dir exist
nano ~/.dbt/profiles.yml

# If terminal complaints, most likely you'll need to create the dir and file.
mkdir ~/.dbt

# Makes the file, copy and paste one of the profiles from the examples, into the file.
nano ~/.dbt/profiles.yml 

# Make any necessary edits.

# To save and exit start with:
Ctrl + X # then follow the prompts.
  ```

3. Recommended - store all data stored in the `/data` directory of the project and named as the utils expects, the utils will automatically pick up the files. You can add a different src dir in the command by using the `--filepath argument`. You’ll see that option in the run step. All files the utils are looking for should exist in the same dir whatever the case may be.

### 3. Run the utils

1. The utils use dbt as well to make a db connection. So you’ll want to run `dbt clean` then `dbt deps` in the terminal. These commands clear out any past dbt packages you have imported brings in the current ones listed in `therootdir/packages.yml`. At this time you do not want any of the study imports uncommented in this file. If you see an error like 'model {model} does not have requred Node...'(at this stage!) this is most likely th problem.

2. Run the utils command(s)
    - These are stored in the `rootdir/scripts dir`. 

#### import_data.py
  - Uses the study config `({study}_study.yaml)` file that defines the projects src data and data dictionaries to create and load tables in your db(db is also defined in the `study config`, should match a profile in your `profiles.yml`). 
  - From the root dir run `./scripts/import_data.py -s {study_id}`
  - The `study_id` should be the same across the board. Whatever used in the study config file.
  - `--filepath` arg is also available, see below.

#### generate_model_docs.py
 - Uses the study config file, and others(…. if something isn’t right ask a teammate), to generate all documents needed for the study, in the correct file locations. 
 - From the root dir run `./scripts/generate_model_docs.py -s {study_id} -p {project_id} -t {tgt_id} -f {optional-alternate data loc}`
 - The `study_id` should be the same across the board. Whatever used in the study config file. 
 - The `project_id` is the org associated with the study, also defined in the study config file.
 - The `tgt_id` is the tgt model identifier for the study. i.e. `tgt_consensus_a`.
 - `--filepath` arg is also available, see below.

### 4. Inspect the run log(s) and the generated directories for errors. 

### 5. Manual edits to files. Listed at the end of the utils generation script log.
  - Find in file references to m00m00( there are ~3 of them). Replace these with your study_id.
  - Edit the `rootdir/packages.yml` to contain any new models in your source dir. 
  - Edit the `dbt_project.yml` files `profile` if needed. The profile should be set to the one set up in the `~/.dbt/profiles.yml`

### 6. Test the run script/dbt commands manually.
  - If the data is imported correctly, You should be able to run the source models. 
    - `dbt clean`
    - `dbt deps`
    - `dbt run {study_id}_stg_{table_id}`
  - You can also run the generated pipeline run script from the root dir  `./{project_id}/scripts/run_{study_id).sh`. This should run the src models, but complain at the ftd and tgt model runs, because the sql in the models are most likely not runnable at this time. dbt does not often give helpful errors when more than one model is run at a time. Suggestion, while editing the pipeline use single commands like in previous step.  The run commands generated are mainly useful for the end product, full pipeline run. Examples of run commands for each stage are seen in `rootdir/scripts/examples/ex_run_commands.sh`. You could also edit this file to run the commands you want to while setting up the project for the first time. 

### Reminder
  - Make sure the the .gitignore files are set up properly before any commits or pushes! If this happens tell someone! Data and secrets must not go to Github and if they do we would need to act.

### Suggestion:
  - Rerun the utils generation if needed. Many docs files are generated with the utils. These can be HARD to keep up with if not generating them programmatically. If one of the data dictionaries has an error, fix the data dictionary, and rerun the generation script. The generation script will not overwrite some files so as to not remove any major work(sql files especially) if you need these regenerated, delete the files manually before re-Running the generation script.

### Resources
#### DuckDB
  - The adapter is currenly being updated if there are any package dependency issues read the docs. [==1.9 documentation](https://docs.getdbt.com/docs/core/connect-data-platform/duckdb-setup)
    - Initial setup dbt-core==1.9.4
    - **From the docs**   *"There is also a database field defined in the DuckDBCredentials class for consistency with the parent Credentials class, but it defaults to main and setting it to be something else will likely cause strange things to happen that cannot be fully predicted, so please avoid changing it."*
