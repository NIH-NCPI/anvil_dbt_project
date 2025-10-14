#!/usr/bin/env python
# coding: utf-8
# %%
# Imports
from pathlib import Path
import os
import pandas as pd
import duckdb
from jinja2 import Template
import sys
import argparse


bucket = os.environ['WORKSPACE_BUCKET']
con = duckdb.connect("/tmp/dbt.duckdb")

from general import *


# %%
def store_study_files(study_files, seeds_files, paths):
    """Store defined files in the bucket. These will persist when the environment is shut down."""
    for file in study_files:
        src_file = f"{paths['src_data_dir']}/{file}"
        dest_file = f"{paths['bucket_study_dir']}"
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

def copy_data_from_bucket(bucket_study_dir, file_list, output_dir):
    for file in file_list:
#         TODO: checkout rsync https://google-cloud-how-to.smarthive.io/buckets/rsync
        # !gsutil cp {bucket_study_dir}/{file} {output_dir}
        print(f'INFO: Copied {file} to {output_dir}') 


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


# %%


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Handle dbt pipeline data")

    parser.add_argument("-s", "--study_id", required=True, help="Study identifier. FTD coded for dbt.")
    parser.add_argument("-o", "--option", required=True, choices=['get_files', 'get_data', 'store_files', 'export_harmonized_data', 'to_utf8'])
    parser.add_argument("-i", "--repo_id", required=False, default='git@github.com:NIH-NCPI/anvil_dbt_project.git', help="SSH version for cloning.")
    parser.add_argument("-r", "--repo", required=False, default='anvil_dbt_project', help="Name of the repo to clone and create dirs for.")
    parser.add_argument("-org", "--org_id", required=False, default='anvil', help="Name of the organization.")
    parser.add_argument("-t", "--tgt_model", required=False, default='tgt_consensus_a', help="Name of the current tgt_consensus model")

    args = parser.parse_args()

    ftd_schema = f'main_{args.study_id}_data'
    tgt_schema = f'main_{args.study_id}_tgt_data'
    
    paths = get_all_paths(args.study_id, args.repo, args.org_id, args.tgt_model, src_data_path=None)

    validation_config = read_file(paths["validation_yml_path"])
    study_config = read_file(paths["study_yml_path"])
    study_config
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
    
    if args.option == 'get_files':
        get_study_files(study_files, seeds_files, paths) 

    if args.option == 'get_data':
        copy_data_from_bucket(paths['bucket_study_dir'], src_table_list, paths['src_data_dir'])
        
    if args.option == 'store_files':
        store_study_files(study_files, seeds_files, paths)


