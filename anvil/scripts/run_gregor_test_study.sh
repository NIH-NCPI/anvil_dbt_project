#!/bin/bash
set -e  # Exit on error
dbt clean
dbt deps || { echo "Error: dbt deps failed. Exiting..."; exit 1; }
# Run Target tables
dbt run --select +tgt_participant --vars '{"source_table": "gregor_test_study_ftd_participant", "target_schema": "gregor_test_study_tgt_data"}'
dbt run --select +tgt_condition --vars '{"source_table": "gregor_test_study_ftd_condition", "target_schema": "gregor_test_study_tgt_data"}'
dbt run --select +tgt_event --vars '{"source_table": "gregor_test_study_ftd_event", "target_schema": "gregor_test_study_tgt_data"}'
dbt run --select +tgt_measurement --vars '{"source_table": "gregor_test_study_ftd_measurement", "target_schema": "gregor_test_study_tgt_data"}'
