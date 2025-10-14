#!/usr/bin/env python
# coding: utf-8
# %%
# Probably no edits necessary

import sys
from pathlib import Path
import argparse

# Probably no edits necessary
import duckdb
import numpy as np
import pandas as pd
from jinja2 import Template
import re
import subprocess
import os
from pathlib import Path
from general import *


# %%
def generate_bq_queries(study_id, src_table_list, query_datasets):

    # Might want to edit but don't change the naming conventions

    """
    IF BQ errors for ARRAYS - use the following syntax in the queries - manual update in BQ.
    ARRAY_TO_STRING(consent_group, ',') AS consent_group,
    """

    for table in src_table_list:
        for ds in query_datasets:
            tablename = ds.split('.', 1)[1]
            print(f"""
            EXPORT DATA OPTIONS(uri='{bucket}/{study_id}/{table}_{tablename}_*.csv', format='CSV', overwrite=true ,header=true) AS 
            (
            SELECT *
            FROM `{ds}.{table}`
            );
            """)


# %%
def union_stg_query(table, file_list, paths):
    """
    """
    sql_queries = []

    table_columns_all = {} 
    all_columns_set = set()

    for table in file_list:
        table_path = paths['study_data_dir'] / table
        table_columns, all_columns = get_column_names([table], paths)

        table_columns_all[str(table_path)] = table_columns[str(table_path)]
        all_columns_set.update(all_columns)

    all_columns_list = sorted(all_columns_set)
    query = generate_stg_query(table_columns_all, all_columns_list, table)
    sql_queries.append(query)

    # Combine all table queries into one formatted SQL string
    final_sql = "\n".join(sql_queries)
    return final_sql


# %%
def generate_stg_query(table_columns, all_columns, table_paths):
    """
    """
    template = Template("""
    {% for table_path, columns in table_columns.items() %}
    SELECT
        {%- for col in all_columns -%}
        {%- set clean_col = col | lower | replace(' ', '_') -%}
        {% if col in columns %}
        "{{ col }}"::TEXT AS "{{ clean_col }}"
        {% else %}
        NULL AS "{{ clean_col }}"
        {%- endif -%}
        {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
    FROM read_csv('{{ table_path }}', AUTO_DETECT=FALSE, HEADER=TRUE, columns={
        {%- for col in columns -%}
        '{{ col }}': 'VARCHAR'
        {% if not loop.last %}, {%- endif -%}
        {%- endfor -%}
    })
    {% if not loop.last %}UNION ALL{%- endif -%}
    {%- endfor %}
    """)

    query = template.render(
        table_columns=table_columns,
        all_columns=all_columns
    )

    return query


# %%


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get metadata for a code using the available locutus OntologyAPI connection.")

    parser.add_argument("-s", "--study_id", required=True, help="Study identifier. FTD coded for dbt.")
    parser.add_argument("-p", "--project_id", required=True, help="Project identifier")
    parser.add_argument("-o", "--options", required=False, default='bq_queries', help="")
    parser.add_argument("-r", "--repo", required=False, default='anvil_dbt_project', help="Name of the repo to clone and create dirs for.")
    parser.add_argument("-t", "--tgt_model", required=False, default='tgt_consensus_a', help="Name of the current tgt_consensus model")
    parser.add_argument("-org", "--org_id", required=False, default='anvil', help="Name of the organization.")

    args = parser.parse_args()

    raw_schema = 'main'

    paths = get_all_paths(args.study_id, args.repo, args.org_id, args.tgt_model, src_data_path=None)
    study_config = read_file(paths["study_yml_path"])

    validation_config = read_file(paths["validation_yml_path"])
    study_config = read_file(paths["study_yml_path"])

    paths = get_terra_paths(args.study_id, args.project_id, args.repo)

    query_datasets = []
    for snapshot, atts in validation_config["datasets"].items():
        query_datasets.append(f"{atts['schema']}.{snapshot}")

    src_table_list = list(study_config["data_dictionary"].keys())

    src_files_list = []
    datasets = validation_config["datasets"].items()
    dataset_names = list(validation_config["datasets"].keys())


    for table in study_config["data_dictionary"].keys():
        for dataset, v in validation_config["datasets"].items():
            f_table = table
            if v['table_name_swap']:
                if f_table in v['table_name_swap'].keys():
                    f_table = v['table_name_swap'].get(table)
            fn = v['filename']
            src_files_list.append(f"{f_table}_{fn}")

    study_files = [f'{args.study_id}_study.yaml', f'{args.study_id}_validation.yaml']

    for dd_table in study_config["data_dictionary"].values():
        study_files.append(dd_table['identifier'])
        if validation_config["bucket_seeds"]:
            for file in validation_config["bucket_data_files"]:
                study_files.append(file)

    seeds_files = []
    if validation_config["bucket_seeds"]:
        for file in validation_config["bucket_seeds"]:
            seeds_files.append(file)
    
    # Compile and format the source 
    src_dds_dict = study_config_dds_to_dict(study_config, paths)
    src_df_names_dict = study_config_df_lists_to_dict(study_config)
    
    if args.options == 'bq_queries':
        generate_bq_queries(args.study_id, src_table_list, query_datasets)
            

    if args.options == 'stg_queries':
        for table, file_list in src_df_names_dict.items():
            print(f'\n\n\n\n\n START {table} QUERY')
            stg_query = union_stg_query(table, file_list, paths)
            print(stg_query)

