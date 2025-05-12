dbt clean
dbt deps  || { echo "Error: dbt deps failed. Exiting..."; exit 1; }
# dbt seed

# src/stg single model dbt command examples
# dbt run --select  m00m00_stg_participant
# dbt run --select  m00m00_stg_condition

# FTD single model dbt command examples
# dbt run --select  m00m00_ftd_participant
# dbt run --select  m00m00_ftd_measurement
# dbt run --select  m00m00_ftd_condition
# dbt run --select  m00m00_ftd_study

# Run a single target model and its dependencies.
# dbt run --select tgt_participant --vars '{"source_table": "m00m00_ftd_participant", "target_schema": "m00m00_tgt_data"}'
# dbt run --select tgt_condition --vars '{"source_table": "m00m00_ftd_condition", "target_schema": "m00m00_tgt_data"}'
# dbt run --select +tgt_event --vars '{"source_table": "m00m00_ftd_event", "target_schema": "m00m00_tgt_data"}'
# dbt run --select +tgt_measurement --vars '{"source_table": "m00m00_ftd_measurement", "target_schema": "m00m00_tgt_data"}'
# dbt run --select tgt_study --vars '{"source_table": "m00m00_ftd_study", "target_schema": "m00m00_tgt_data"}'

# gregor/duckdb examples
dbt run --select  gregor_test_study_stg_participant
dbt run --select  gregor_test_study_stg_phenotype
# dbt run --select  gregor_test_study_ftd_participant
# dbt run --select  gregor_test_study_ftd_phenotype
# dbt run --select tgt_participant --vars '{"source_table": "gregor_test_study_ftd_participant", "target_schema": "main"}'
# dbt run --select tgt_phenotype --vars '{"source_table": "gregor_test_study_ftd_phenotype", "target_schema": "main"}'