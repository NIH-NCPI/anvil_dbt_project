# +
import duckdb
import pandas as pd
import numpy as np
import sys
from jinja2 import Template
import re
import subprocess
import os
from pathlib import Path

from dbt_pipeline_utils.scripts.helpers.general import *

bucket = os.environ['WORKSPACE_BUCKET']
engine = duckdb.connect("/tmp/dbt.duckdb")

# +
from pathlib import Path
from dbt_pipeline_utils.scripts.helpers.general import *

bucket = os.environ['WORKSPACE_BUCKET']
engine = duckdb.connect("/tmp/dbt.duckdb")

def get_terra_paths(study_id, project_id, dbt_repo):
    """
    For automatic validation of dir path creation, end the dir variables with "dir"
    """
    script_dir = Path.cwd().resolve()
    home_dir = script_dir.parent.parent.parent
    pipeline_dir = home_dir / 'pipeline'
    repo_home_dir =  pipeline_dir / dbt_repo # user editable location for the pipeline repo
    validation_yml_path = repo_home_dir / 'data' / study_id / f'{study_id}_validation.yaml'
    output_dir = pipeline_dir / 'output_data'
    output_study_dir = output_dir / study_id
    output_validation_dir = output_study_dir / "validation"
    seeds_dir = repo_home_dir / 'seeds'
    notebook_dir = repo_home_dir / 'notebooks'
    
    dbt_dir = home_dir / ".dbt" # New loc for the profiles.yml
    ssh_dir = home_dir / ".ssh"
    git_config_path = home_dir / ".gitconfig"
    id_rsa_src = home_dir / "id_rsa"
    id_rsa_dest = ssh_dir / "id_rsa"
    bash_profile = home_dir / ".bash_profile"
    terra_gitignore = home_dir / 'gitignore_global'
    bucket_study_dir = f'{bucket}/{study_id}'
    
    return {
        "home_dir": home_dir,
        "repo_home_dir": repo_home_dir,
        "validation_yml_path": validation_yml_path,
        "pipeline_dir": pipeline_dir,
        "output_dir": output_dir,
        "output_study_dir":output_study_dir,
        "output_validation_dir": output_validation_dir,
        "seeds_dir": seeds_dir,
        "notebook_dir": notebook_dir,
        "dbt_dir": dbt_dir,
        "ssh_dir": ssh_dir,
        "git_config_path": git_config_path,
        "id_rsa_src": id_rsa_src,
        "id_rsa_dest": id_rsa_dest,
        "bash_profile": bash_profile,
        "terra_gitignore": terra_gitignore,
        "bucket_study_dir": bucket_study_dir,
        "bucket": bucket,
    }

def get_all_paths(study_id, dbt_repo, org_id, tgt_model_id=None, src_data_path=None):
    """
    Creates one dictionary of frequently used paths. 
    
    The Terra paths are specific to directories required for Terra development. 
    The pipeline_utils paths cover the paths within the project repository.
    """
    original_cwd = Path().resolve()
    one_dir_back = original_cwd.parent # pipeline_utils get_paths needs to be run from the root dir

    paths = {}
    paths.update(get_terra_paths(study_id, org_id, dbt_repo))  # Terra paths
    
    os.chdir(one_dir_back)
    paths.update(get_paths(study_id, org_id, tgt_model_id, src_data_path))  # pipeline_utils paths
    os.chdir(original_cwd)

    return paths
# +
def execute(query):
    """
    Connect to duckdb, execute a query and format as a DataFrame with headers. 
    """
    result = engine.execute(query)
    df = pd.DataFrame(result.fetchall(), columns=[col[0] for col in result.description])
    return df

def study_config_dds_to_dict(study_config, paths):
    """
    Read in the src data dictionaries and put them into a dictionary. The key is the 
    table name (i.e. 'subject') and the value is the datadictionary as a pd DataFrame.
    """
    src_dds_dict = {}
    for table_name, table_info in study_config["data_dictionary"].items():
        src_dds_dict[table_name] = table_info['identifier']
        src_dds_dict[table_name] = read_file(paths["src_data_dir"] / table_info['identifier'])
    return src_dds_dict

def study_config_df_lists_to_dict(study_config):
    """
    Create a dictionary of tables and the src data files associated with the table. The key is 
    the table_name (i.e. 'subject') and the value is a list of filenames.
    """
    src_dfs_dict = {}
    for table_name, table_info in study_config["data_files"].items():
        src_dfs_dict[table_name] = table_info['identifier']
    return src_dfs_dict


def get_separate_src_tables_dict(src_df_names_dict, tablename, paths):
    """
    Create a dictionary for source datafile storage. The key is 
    the file name (i.e. 'subject_ANVIL_consent') and the value is the datafile as pd DataFrame.
    """
    separate_src_tables_dict = {}
    
    for table, file_list in src_df_names_dict.items():
        if table == tablename:
            for file in file_list:

                table_path = paths['src_data_dir'] / file 
                table_columns, _ = get_column_names(file_list, paths)
                columns = table_columns[str(table_path)]

                column_definitions = ", ".join([f"'{col}': 'VARCHAR'" for col in columns])  # Fix: Use proper dictionary syntax
                query = f"""
                SELECT * FROM read_csv('{table_path}', AUTO_DETECT=FALSE, HEADER=TRUE, columns={{ {column_definitions} }})
                """
                result = engine.execute(query)

                df = pd.DataFrame(result.fetchall(), columns=[col[0] for col in result.description])

                separate_src_tables_dict[file.replace('_000000000000.csv','')] = df  
    return separate_src_tables_dict


def union_tables(src_dfs_dict, paths):
    """
    Unions all src data files grouped by table type/group(i.e. 'sample'). The key is 
    the table_name (i.e. 'subject') and the value is the data as a pd Dataframe.
    """
    unioned_dfs_dict = {}

    for table_name, src_tables in src_dfs_dict.items():
        table_columns_all = {} 
        all_columns_set = set()

        for table in src_tables:
            table_path = paths['src_data_dir'] / table
            table_columns, all_columns = get_column_names([table], paths)

            table_columns_all[str(table_path)] = table_columns[str(table_path)]
            all_columns_set.update(all_columns)

        all_columns_list = sorted(all_columns_set)
        query = generate_union_query(table_columns_all, all_columns_list, src_tables)

        df = execute(query)
        unioned_dfs_dict[table_name] = df

    return unioned_dfs_dict

def get_column_names(file_list, paths):
    """
    Get unique column names from the files in the list.
    """
    all_columns = set()
    table_columns = {}

    for table in file_list:
        table_path = paths['src_data_dir'] / table 

        query = f"""
        SELECT column_name AS name
        FROM (DESCRIBE SELECT * FROM read_csv_auto('{table_path}'))
        """
        result = engine.execute(query)

        columns = [row[0] for row in result.fetchall()]
        table_columns[str(table_path)] = columns
        all_columns.update(columns)

    sorted_columns = sorted(all_columns)
    return table_columns, sorted_columns

def generate_union_query(table_columns, all_columns, table_paths):
    """
    Generates a sql query that will allow unioning data without matching column names.
    """
    template = Template("""
    {% for table, columns in table_columns.items() %}
    SELECT 
        {% for col in all_columns %}
        COALESCE({% if col in columns %}{{ col }}{% else %}NULL{% endif %}, NULL) AS {{ col }}{% if not loop.last %}, {% endif %}
        {% endfor %}
    FROM '{{ table }}'
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
    """)

    query = template.render(
        table_columns=table_columns,
        all_columns=all_columns
    )

    return query

def characterization_report(df, table_id_col):
    """
    Create a dataframe that shows the number of records available in a table's column
    per table type/group.
    """
    columns_to_analyze = [col for col in df.columns if col != table_id_col]
    
    report_data = []
    
    grouped = df.groupby(table_id_col)
    
    for src_table, group in grouped:
        for col in columns_to_analyze:
            count = group[col].notnull().sum()
            
            report_data.append({
                'src_table': src_table,
                'src_data_column': col,
                'n_': count
            })
    
    report_df = pd.DataFrame(report_data)
    
    pivoted_report = report_df.pivot_table(
        index='src_data_column', 
        columns='src_table', 
        values=['n_'], 
        aggfunc='sum'
    )

    pivoted_report.columns = [f"{val}_{src_table}" for val, src_table in pivoted_report.columns]
    
    pivoted_report = pivoted_report.reset_index()
    
    return pivoted_report

def parse_enum_values(enum_str):
    """
    Parse the enumerations and return unique values
    """
    if pd.isna(enum_str):
        return set()
    return set(re.split(r'[;|]', str(enum_str)))

def get_datafile_enums(df):
    """
    Create a dictionary with column names as keys and unique enumerations as values.
    """
    column_enumerations = {}

    for col in df.columns:
        enumerations = set()
        
        for value in df[col].dropna().unique():
            enumerations.update(parse_enum_values(value))
        
        column_enumerations[col] = enumerations
    
    return column_enumerations


def get_datadictionary_enums(df):
    """
    Create a dictionary with column names as keys and unique enumerations as values.
    """
    enum_dict = {}
    for col_name, row in df.iterrows():
        enum_values = parse_enum_values(row['enumerations'])
        enum_dict[row['variable_name']] = enum_values
    return enum_dict

def compare_column_enumerations(dd_dict, df_dict):
    """
    Compare two dictionaries of column enumerations and output a DataFrame.
    """
    comparison_data = []

    common_columns = set(dd_dict.keys()).intersection(set(df_dict.keys()))

    for col in common_columns:
        set1 = dd_dict[col]
        set2 = df_dict[col]

        missing_from_dd = set2 - set1
        missing_from_df = set1 - set2

        comparison_data.append({
            'column_name': col,
            'df_enum_missing_from_dd': "| |".join(missing_from_dd) if missing_from_dd else None,
            'dd_enum_missing_from_df': "| |".join(missing_from_df) if missing_from_df else None,
            'df_enums': "| |".join(set2),  
            'dd_enums': "| |".join(set1),
    
        })

    comparison_df = pd.DataFrame(comparison_data, columns=['column_name',
                                                           'df_enum_missing_from_dd',
                                                           'dd_enum_missing_from_df',
                                                           'df_enums',
                                                           'dd_enums'
                                                           ])

    return comparison_df


def schema_comparison(unioned_dfs_dict,src_dds_dict):
    """
    Handles combining characterization reports into one dataframe
    """
    reports = {}
    for table_name, df in unioned_dfs_dict.items():
        if table_name == 'anvil_dataset':
            continue

        report_df = characterization_report(df, 'ingest_provenance')

        dd_col = pd.DataFrame(src_dds_dict[table_name][['variable_name']].rename(columns={'variable_name': 'dd_column'}))
        merged = dd_col.merge(report_df,
                    how='outer',
                    left_on='dd_column',
                    right_on='src_data_column').reset_index(drop=True).replace({np.nan: None})

        columns_to_convert = merged.columns[2:]
        for col in columns_to_convert:
            merged[col] = merged[col].fillna(0).astype(int)

        reports[table_name] = merged

    return reports

def enum_report_by_file(src_dds_dict, src_df_names_dict, paths):
    """
    Run the column comparison report at the single file level.
    """
    all_results = []

    for table_name in src_dds_dict.keys():
        if table_name == 'anvil_dataset':
                continue
        separate_src_tables_dict = get_separate_src_tables_dict(src_df_names_dict, table_name, paths)
        enum_cols_dd = src_dds_dict[table_name][src_dds_dict[table_name]['data_type'] == 'enumeration']
        df_col_names = enum_cols_dd['variable_name'].tolist()
        
        if df_col_names:
            for file in separate_src_tables_dict:
                enum_cols_df = separate_src_tables_dict[file][separate_src_tables_dict[file].columns.intersection(df_col_names)]

                dd = src_dds_dict[table_name]
                dd_enums = get_datadictionary_enums(dd)
                df_enums = get_datafile_enums(enum_cols_df)

                comparison_results = compare_column_enumerations(dd_enums, df_enums)
                comparison_results['src_df'] = file 
                comparison_results['src_table'] = table_name 

                all_results.append(comparison_results)
        results_df = pd.concat(all_results, ignore_index=True)

    return results_df
                

def enum_report_by_table_group():
    """
    Run the column comparison report at the table level. - More summarized view.
    """
    all_results = []
    for table_name in src_dds_dict.keys():

        if table_name == 'anvil_dataset':
            continue

        enum_cols_dd = src_dds_dict[table_name][src_dds_dict[table_name]['data_type'] == 'enumeration']
        df_col_names = enum_cols_dd['variable_name'].tolist()
        if df_col_names:
            enum_cols_df = unioned_dfs_dict[table_name][unioned_dfs_dict[table_name].columns.intersection(df_col_names)]

        dd = src_dds_dict[table_name]
        dd_enums = get_datadictionary_enums(dd)

        df_enums = get_datafile_enums(enum_cols_df)

        comparison_results = compare_column_enumerations(dd_enums, df_enums)
        comparison_results['src_table'] = table_name 

        all_results.append(comparison_results)
      
    results_df = pd.concat(all_results, ignore_index=True)

    return results_df

def format_not_nulls(s):
    """
    If the record is not null, color the text darkred
    """
    return ['color: darkred' if v is not None else '' for v in s]
def format_nulls(s):
    """
    If the record is null, color the text red
    """
    return ['color: red' if v is None else '' for v in s]

# +
# pipeline helpers 
def create_file_dict(table, count):
    file_list = []
    for i in range(count):
        if i == 0:
            file = f'{table}_{"0" * 12}.csv'
        else:
            file = f'{table}_{"0" * (12 - len(str(i)))}{i}.csv'
        file_list.append(file)
    
    return {table: file_list}

# def get_bucket_src_data_format_store(src_table_list, src_files, partial_file_dicts, paths):
#     '''
#     Data files are a special case. Get them from the bucket with this 
#     function, NOT pull_study_files()
#     Data files should not be edited manually. If edits are required, 
#     use the original queries with edits to store the new data in the bucket
#     '''
#     print('INFO: Start')

#     copy_data_from_bucket(paths['bucket_study_dir'], src_files, paths['src_data_dir'])

#     # Iterate over the dictionaries and process files
#     for file_dict in partial_file_dicts:  # Iterate over each dictionary in partial_file_dicts
#         for table, file_list in file_dict.items():  # Extract table name (key) and file list (value)
#             # Concatenate files for the current table
#             read_and_concat_files(file_list, paths['src_data_dir'], f'{paths['src_data_dir']}/{table}_combined.csv')

#     # Rename the concatenated files
#     for table in src_table_list:  # Iterate over all table names
#         rename_file_single_dir(paths['src_data_dir'], f'{table}_combined.csv', f'{table}.csv')

#     # Remove all the files in the dictionaries
#     for file_dict in partial_file_dicts:  # Iterate over each dictionary in partial_file_dicts
#         for table, file_list in file_dict.items():
#             remove_file(file_list, paths['src_data_dir'])

#     print('INFO: Complete')
    
def setup_ssh(paths):
    # Create and configure ~/.ssh
    if not paths['ssh_dir'].is_dir():
        paths['ssh_dir'].mkdir(mode=0o700, exist_ok=True)
        print("INFO: Created ~/.ssh directory.")
    ssh_config = paths['ssh_dir'] / "config"
    if not ssh_config.exists():
        ssh_config.write_text(
            """# SSH configuration for GitHub
Host github
  HostName github.com
  User git
  IdentityFile ~/.ssh/id_rsa
  IdentitiesOnly yes
"""
        )
        ssh_config.chmod(0o600)
        print("INFO: Created ~/.ssh/config file.")
        
 # Move id_rsa to ~/.ssh and set correct permissions
    if paths['id_rsa_src'].exists():
        os.system(f"mv {paths['id_rsa_src']} {paths['id_rsa_dest']}")
        id_rsa_dest.chmod(0o600)
        print(f"INFO: Moved id_rsa to {paths['id_rsa_src']} and set permissions to 600.")

    if not paths['id_rsa_src'].exists() and not paths['id_rsa_dest'].exists():
        print(f"WARNING: Make sure the private key is available.")

# See [docs](https://github.com/DataBiosphere/terra-examples/blob/main/best_practices/source_control/terra_source_control_cheatsheet.md#1-use-the-jupyter-console-to-upload-your-github-ssh-key-and-create-an-interactive-terminal-session) 
def setup_gh(gh_user, gh_email, paths):
    content1=f'''
[user]
        email = {gh_email}
        name = {gh_user}
[url "git@github.com:"]
        insteadOf = https://github.com/

    '''

    with paths['git_config_path'].open("a") as file:
        file.write("\n" + content1)

    print("INFO: Edited ~/.gitconfig file.")
        
def update_bash_profile(paths, pipeline):
    
    content1 = """
# Custom PS1 prompt with virtual environment display
export PS1='\\[\\033[1;33m\\]${VIRTUAL_ENV:+(venv)} \\[\\033[1;36m\\]$(basename "$PWD")\\[\\033[00m\\]\\$ '
"""

    content2 = f"""
# Add SSH private key
eval "ssh-add ~/.ssh/id_rsa"

# Alias to activate Python virtual environment
alias activate="source /home/jupyter/venv-python3.12/bin/activate"

# Alias to setup ssh and permissions:
alias setup_ssh="
echo 'Assuming the id_rsa is in the {paths['id_rsa_src']} dir'
eval 'mv {paths['id_rsa_src']} {paths['id_rsa_dest']}'
eval 'chmod 600 ~/.ssh/id_rsa'
"

# Setup dirs and clone the repo
alias clone_repo="
eval 'mkdir {paths['pipeline_dir']}'
eval 'mkdir {paths['repo_home_dir']}'
eval 'git clone {pipeline} {paths['repo_home_dir']}'
eval 'mkdir paths['src_data_dir']'
eval 'activate'
eval 'cd {paths['repo_home_dir']}'
eval 'mkdir {paths['output_dir']}'
eval 'mkdir {paths['output_study_dir']}'
"

# Alias to dbt prep file system:
alias setup_data="
eval 'mkdir {paths['dbt_dir']}'
eval 'cp {paths['profiles_path_root']} {paths['profiles_path_home']}'
"

# Alias to clean and compile pipeline:
alias r_dbt="
eval 'dbt clean'
eval 'dbt deps'
"

echo 'Alias are: activate, r_dbt, setup_ssh clone_repo, setup_data'

export LOCUTUS_LOGLEVEL='INFO'

"""
    with paths['bash_profile'].open("a") as file:
        file.write("\n" + content1 + "\n" + content2)
        
    print("INFO: Content successfully added to ~/.bash_profile.")

    print("INFO: To apply changes, run: source ~/.bash_profile")

def stop_gitignoring_sql_files(paths):
    content = """
!*.sql
!*.yml
"""
    with paths['terra_gitignore'].open("a") as file:
        file.write("\n" + content + "\n")
    print("INFO: Content successfully added to ~/gitignore_global")
    
    
def run_initial_setup(paths, gh_user, gh_email, pipeline):
    '''
    Run the setup functions
    '''
    setup_ssh(paths) # Required first time env setup
    setup_gh(gh_user, gh_email, paths) # Required first time env setup
    update_bash_profile(paths, pipeline)
    stop_gitignoring_sql_files(paths)
def copy_data_from_bucket(bucket_study_dir, file_list, output_dir):
    for file in file_list:
#         TODO: checkout rsync https://google-cloud-how-to.smarthive.io/buckets/rsync
        # !gsutil cp {bucket_study_dir}/{file} {output_dir}
        print(f'INFO: Copied {file} to {output_dir}') 

def copy_data_to_bucket(bucket_study_dir, file_list, input_dir):
    for file in file_list:
        # !gsutil cp {input_dir} {bucket_study_dir}/{file}
        print(f'INFO: Copied {file} to the bucket') 
        
# Read and concatenate all files
def read_and_concat_files(file_list, input_dir, output_dir):
    dfs = [pd.read_csv(f'{input_dir}/{file}') for file in file_list] 
    combined_subject = pd.concat(dfs, ignore_index=True)
    combined_subject.to_csv(output_dir, index=False)
    
def rename_file_single_dir(d_dir, input_fn, output_fn):
    # clean up data_dir
    # !mv {d_dir}/{input_fn} {d_dir}/{output_fn}
    return

def remove_file(file_list, d_dir):
    for file in file_list:
        file_path = os.path.join(d_dir, file)
        try:
            os.remove(file_path)
            print(f'INFO: Processed: {file}')
        except FileNotFoundError:
            print(f'WARNING: File not found: {file}')
        except Exception as e:
            print(f'ERROR: Could not remove {file} due to {e}')
    return
    
    
def store_study_files(study_files, seeds_files, paths):
    """Store defined files in the bucket. These will persist when the environment is shut down."""
    for file in study_files:
        src_file = f"{paths['src_data_dir']}/{file}"
        dest_file = f"{paths['bucket_study_dir']}"
        display(src_file)
        display(dest_file)
        subprocess.run(["gsutil", "cp", src_file, dest_file], check=True)
    
    for file in seeds_files:
        src_file = f"{paths['seeds_dir']}/{file}"
        dest_file = f"{paths['bucket_study_dir']}"
        subprocess.run(["gsutil", "cp", src_file, dest_file], check=True)
    return
       
def get_study_files(study_files, seeds_files, paths):
    """Store defined files in the bucket. These will persist when the environment is shut down."""
    for file in study_files:
        dest_file = f"{paths['src_data_dir']}"
        src_file = f"{paths['bucket_study_dir']}/{file}"
        subprocess.run(["gsutil", "cp", src_file, dest_file], check=True)
    
    for file in seeds_files:
        dest_file = f"{paths['seeds_dir']}"
        src_file = f"{paths['bucket_study_dir']}/{file}"
        subprocess.run(["gsutil", "cp", src_file, dest_file], check=True)  
    return
    
# Export functions       
def get_tables_from_schema(schema):
    '''
    Get tables from a duckdb dataset. 
    '''
    result = engine.execute(f"""
    SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'
    """)
    r = pd.DataFrame(result.fetchall(), columns=[col[0] for col in result.description])
    return r['table_name'].to_list()

def tables_to_output_dir(tables, tgt_schema, paths):
    for t in tables:
        name = Path(t).stem.replace(f'tgt_','')
        t = engine.execute( f"COPY (SELECT * FROM {tgt_schema}.{t}) TO '{paths['output_study_dir']}/{name}.csv' (HEADER, DELIMITER ',')").fetchall()
        print(name)

def harmonized_to_bucket(tables, paths):
    for t in tables:
        name = Path(t).stem.replace(f'tgt_','')
        # !gsutil cp {paths['output_study_dir']}/{name}.csv {paths['bucket']}/harmonized/{study_id}
        print(name)


def copy_to_csv_and_export_to_bucket(tgt_schema, paths):    
    '''
    Get the tables that you want to export to csv.
    Then export to csv in the output dir
    '''
    tgt_tables = get_tables_from_schema(tgt_schema)

    tables_to_output_dir(tgt_tables, tgt_schema, paths)
    display('Tables sent to output.')
    
    harmonized_to_bucket(tgt_tables)
    display('csvs sent to bucket')
    
def convert_csv_to_utf8(input_file_path, output_filepath, delimiter, encoding):
    df = pd.read_csv(input_file_path, encoding=encoding, delimiter=delimiter, quoting=3)
    df.to_csv(output_filepath, index=False, encoding='utf-8')
    print(f"Converted CSV saved to {output_filepath}")
    
def study_config_datasets_to_dict(study_config, paths):
    """
    Read in the src data dictionaries and put them into a dictionary. The key is the 
    table name (i.e. 'subject') and the value is the datadictionary as a pd DataFrame.
    """
    src_dds_dict = {}
    for table_name, table_info in study_config["data_dictionary"].items():
        src_dds_dict[table_name] = table_info['identifier']
        src_dds_dict[table_name] = read_file(paths["src_data_dir"] / table_info['identifier'])
    return src_dds_dict
# -


