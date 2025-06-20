dbt clean
dbt deps  || { echo "Error: dbt deps failed. Exiting..."; exit 1; }
# dbt seed
# 

# gregor/duckdb examples
dbt run --select  gregor_synthetic_stg_participant
dbt run --select  gregor_synthetic_stg_phenotype

# dbt run --select  gregor_synthetic_ftd_accesspolicy
# dbt run --select  gregor_synthetic_ftd_demographics
dbt run --select  gregor_synthetic_ftd_demographics

# dbt run --select tgt_participant --vars '{"source_table": "gregor_synthetic_ftd_participant", "target_schema": "main"}'
# dbt run --select tgt_phenotype --vars '{"source_table": "gregor_synthetic_ftd_phenotype", "target_schema": "main"}'

# Run a single target model and its dependencies.
# dbt run --select tgt_participant --vars '{"source_table": "gregor_synthetic_ftd_participant", "target_schema": "gregor_synthetic_tgt_data"}'
# dbt run --select tgt_condition --vars '{"source_table": "gregor_synthetic_ftd_condition", "target_schema": "gregor_synthetic_tgt_data"}'
# dbt run --select +tgt_event --vars '{"source_table": "gregor_synthetic_ftd_event", "target_schema": "gregor_synthetic_tgt_data"}'
# dbt run --select +tgt_measurement --vars '{"source_table": "gregor_synthetic_ftd_measurement", "target_schema": "gregor_synthetic_tgt_data"}'
# dbt run --select tgt_study --vars '{"source_table": "gregor_synthetic_ftd_study", "target_schema": "gregor_synthetic_tgt_data"}'
