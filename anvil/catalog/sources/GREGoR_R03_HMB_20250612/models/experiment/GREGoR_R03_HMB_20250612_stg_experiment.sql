{{ config(materialized='table') }}

    with source as (
        select 
        "experiment_id"::text as "experiment_id",
       "table_name"::text as "table_name",
       "id_in_table"::text as "id_in_table",
       "participant_id"::text as "participant_id"
        from {{ source('GREGoR_R03_HMB_20250612','GREGoR_R03_HMB_20250612_experiment') }}
    )

    select 
        ROW_NUMBER() OVER () AS ftd_index
        ,source.*
        from source
    