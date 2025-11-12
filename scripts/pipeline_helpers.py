#!/usr/bin/env python
import duckdb
from pathlib import Path
import os
import subprocess
import argparse
from general import get_all_paths
from dbt_pipeline_utils.scripts.helpers.general import read_file
from dbt_pipeline_utils import logger

bucket = os.environ["WORKSPACE_BUCKET"]
engine = duckdb.connect("/tmp/dbt.duckdb")


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
        # TODO: checkout rsync https://google-cloud-how-to.smarthive.io/buckets/rsync
        subprocess.run(
            ["gsutil", "cp", f"{bucket_study_dir}/{file}", output_dir], check=True
        )
        logger.info(f"INFO: Copied {file} to {output_dir}")


# Export functions
def get_tables_from_schema(schema):
    """
    Get tables from a duckdb dataset.
    """
    result = engine.execute(
        f"""
    SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'
    """
    )
    r = pd.DataFrame(result.fetchall(), columns=[col[0] for col in result.description])
    return r["table_name"].to_list()


def tables_to_output_dir(tables, tgt_schema, paths):
    for t in tables:
        name = Path(t).stem.replace(f"tgt_", "")
        t = engine.execute(
            f"COPY (SELECT * FROM {tgt_schema}.{t}) TO '{paths['output_study_dir']}/{name}.csv' (HEADER, DELIMITER ',')"
        ).fetchall()
        logger.info(name)


def harmonized_to_bucket(tables, paths):
    for t in tables:
        name = Path(t).stem.replace(f"tgt_", "")
        # !gsutil cp {paths['output_study_dir']}/{name}.csv {paths['bucket']}/harmonized/{study_id}
        logger.info(name)


def copy_to_csv_and_export_to_bucket(tgt_schema, paths):    
    '''
    Get the tables that you want to export to csv.
    Then export to csv in the output dir
    '''
    tgt_tables = get_tables_from_schema(tgt_schema)

    tables_to_output_dir(tgt_tables, tgt_schema, paths)
    logger.info("Tables sent to output.")

    harmonized_to_bucket(tgt_tables)
    logger.info("csvs sent to bucket")


def main():
    parser = argparse.ArgumentParser(description="Handle dbt pipeline data")

    parser.add_argument("-s", "--study_id", required=True, help="Study identifier. FTD coded for dbt.")
    parser.add_argument("-o", "--option", required=True, choices=['get_files', 'get_data', 'store_files', 'export_harmonized_data'])
    parser.add_argument("-i", "--repo_id", required=False, default='git@github.com:NIH-NCPI/anvil_dbt_project.git', help="SSH version for cloning.")
    parser.add_argument("-r", "--repo", required=False, default='anvil_dbt_project', help="Name of the repo to clone and create dirs for.")
    parser.add_argument("-org", "--org_id", required=False, default='anvil', help="Name of the organization.")
    parser.add_argument("-t", "--tgt_model", required=False, default='tgt_consensus_a', help="Name of the current tgt_consensus model")

    args = parser.parse_args()

    paths = get_all_paths(args.study_id, args.repo, args.org_id, args.tgt_model, src_data_path=None)

    validation_config = read_file(
        paths["src_data_dir"] / Path(f"{args.study_id}_validation.yaml")
    )
    study_config = read_file(
        paths["src_data_dir"] / Path(f"{args.study_id}_study.yaml")
    )

    src_table_list = list(study_config["data_dictionary"].keys())

    src_files_list = []
    ori_src_files_list = []
    for table in study_config["data_dictionary"].keys():
        for dataset, v in validation_config["datasets"].items():
            f_table = table
            fn = v["filename"]
            ori_src_files_list.append(f"{f_table}_{fn}")
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
        copy_data_from_bucket(
            paths["bucket_study_dir"], ori_src_files_list, paths["src_data_dir"]
        )

    if args.option == 'store_files':
        store_study_files(study_files, seeds_files, paths)


if __name__ == "__main__":
    main()
