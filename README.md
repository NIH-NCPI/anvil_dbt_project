bash
# anvil_dbt_pipeline

## Procedure
**REMINDER:** Before any commit! Make sure the the .gitignore files are set up properly before making any commits! They should already be set up and no edits needed, but verify this! 

# Local setup
Recommended, if running on your local computer, create a venv, and activate it. Instructions below for Terra development.

1. Install the project requirements with `pip install -r requirements.txt` from the root dir.  This will install the utils package, among other dbt required installations.
- NOTE: If there are issues with a pkg not installing use `pip install --force-reinstall --no-cache-dir -r requirements.txt`

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

3. Edit the rootdir dbt_project.yml `profile`. This should be set to the name of the profile created for your database in the profiles.yaml.

4.  Store data
 - See `rootdir/data/data_expectations_utils` for an example of the data that should be stored at this time. More information should be found in the README of the data dir as well.
 - Store all data stored in the `/data` directory of the project and named as the utils expects, the utils will automatically pick up the files. You can add a different src dir in the command by using the `--filepath argument`. You’ll see that option in the run step. All files the utils are looking for should exist in the same dir whatever the case may be.

5. Run the utils

 -  Make sure the `rootdir/dbt_project.yml` `profile` identifies the dbt db profile required for your study.
 - The utils use dbt as well to make a db connection. So you’ll want to run `dbt clean` then `dbt deps` in the terminal. These commands clear out any past dbt packages you have imported brings in the current ones listed in `therootdir/packages.yml`. At this time you do not want any of the study imports uncommented in this file. If you see an error like 'model {model} does not have requred Node...'(at this stage!) this is most likely th problem.

Available commands:

 - import_data (Optional)
    - Uses the study config `({study}_study.yaml)` file that defines the projects src data and data dictionaries to create and load tables in your db(db is also defined in the `study config`, should match a profile in your `profiles.yml`). 
    - NOTE: Some databases and dbt pipelines dont require this command to be run. Ex: When using duckdb and importing data directly, via the first dbt model.
    - From the root dir run `import_csv_data -s {study_id}` -p {project_id} -t {tgt_id} -f {optional-alternate data loc}`
    - The `study_id` should be the same across the board. Whatever used in the study config file. 
    - The `project_id` is the org associated with the study, also defined in the study config file.
    - The `tgt_id` is the tgt model identifier for the study. i.e. `tgt_consensus_a`.
    - `--filepath` arg is also available, see below.

 - generate_model_docs
    - Uses the study config file, and others(…. if something isn’t right ask a teammate), to generate all documents needed for the study, in the correct file locations. 
    - From the root dir run `generate_model_docs -s {study_id} -p {project_id} -t {tgt_id} -f {optional-alternate data loc}`
    - The `study_id` should be the same across the board. Whatever used in the study config file. 
    - The `project_id` is the org associated with the study, also defined in the study config file.
    - The `tgt_id` is the tgt model identifier for the study. i.e. `tgt_consensus_a`.
    - `--filepath` arg is also available, see below.

 - Inspect the run log(s) and the generated directories for errors. 

 - Manual edits to files. Listed at the end of the utils generation script log.
  -  Edit the `rootdir/packages.yml` to contain any new models in your source dir. 

- Test the run script/dbt commands manually.
  - If the data is imported correctly, You should be able to run the source models. 
    - `dbt clean`
    - `dbt deps`
    - `dbt run {study_id}_stg_{table_id}`
  - You can also run the generated pipeline run script from the root dir  `./{project_id}/scripts/run_{study_id).sh`. This should run the src models, but complain at the ftd and tgt model runs, because the sql in the models are most likely not runnable at this time. dbt does not often give helpful errors when more than one model is run at a time. Suggestion, while editing the pipeline use single commands like in previous step.  The run commands generated are mainly useful for the end product, full pipeline run. Examples of run commands for each stage are seen in `rootdir/scripts/examples/ex_run_commands.sh`. You could also edit this file to run the commands you want to while setting up the project for the first time. 

## Reminder
  - Make sure the the .gitignore files are set up properly before any commits or pushes! If this happens tell someone! Data and secrets must not go to Github and if they do we would need to act.

## Suggestion:
  - Rerun the utils generation if needed. Many docs files are generated with the utils. These can be HARD to keep up with if not generating them programmatically. If one of the data dictionaries has an error, fix the data dictionary, and rerun the generation script. The generation script will not overwrite some files so as to not remove any major work(sql files especially) if you need these regenerated, delete the files manually before re-Running the generation script.



# Terra Setup

## Workspace setup
1. Make new Terra workspace
2. Place anvil_dbt_project/scripts/startup_anvil_dbt.sh into the workspace bucket and copy its gsutil URI.
3. Start an environment. Put the gsutil URI of the startup script into the startup script field and start the env(~20 min).
4. Download, then place anvil_dbt_project/scripts/harmonization_initial_setup.py in the 'home/jupyter'dir.
5. Place your id_rsa key file in the 'home/jupyter' dir. Note: harmonization_initial_setup.py will move the file to the correct dir, then follow the Terra instructions for setting up credentials with GitHub automatically. The key is deleted upon environment deletion. See Terra docs.
6. Run python harmonization_initial_setup.py -s {study_id} -p {project_id} -u {github username} -e {github email} from 'home/jupyter'
7. In the Terminal:
  - Run: source ~/.bash_profile
  - Run: setup_ssh
  - Run: clone_repo - When asked, answer 'yes'
  - Run: setup_data

## Pipeline setup
1. Fill out config file(s). Various example files are found in `data/static/example_data`
2. Place the config files in the `anvil_dbt_project/data/{study_id}` directory.
3. Place files(data dictionaries, etc) in the workspace bucket. These should be listed in the config files.
4. Place datafiles (sample_anvil_dataset_filename, etc) in the workspace bucket. These should be listed in the config files.
5. Place any seed files in the workspace bucket.  These should be listed in the config files.
6. Pull the avaiable data into or outof the workspace bucket with: python scripts/pipeline_helpers.py -s cmg_yale -o {Choices: store_files, get_files, get_data}
```
