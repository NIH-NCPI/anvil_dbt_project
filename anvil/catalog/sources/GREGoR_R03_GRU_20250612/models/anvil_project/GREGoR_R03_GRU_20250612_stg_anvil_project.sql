{{ config(materialized='table') }}

    with source as (
        select 
        "project_id"::text as "project_id",
       "funded_by"::text as "funded_by",
       "generated_dataset_id"::text as "generated_dataset_id",
       "principal_investigator"::text as "principal_investigator",
       "title"::text as "title",
       "registered_identifier"::text as "registered_identifier",
       "source_datarepo_row_ids"::text as "source_datarepo_row_ids"
        from {{ source('GREGoR_R03_GRU_20250612','GREGoR_R03_GRU_20250612_anvil_project') }}
    )

    select 
        ROW_NUMBER() OVER () AS ftd_index
        ,source.*
        from source
    