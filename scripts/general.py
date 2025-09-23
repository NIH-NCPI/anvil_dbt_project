# +
import duckdb
import pandas as pd
import numpy as np
import sys
from jinja2 import Template
import re
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

# -



