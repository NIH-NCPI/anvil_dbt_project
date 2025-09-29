

with source as (
    select 
      "consent_group"::text as "consent_group",
       "data_use_permission"::text as "data_use_permission",
       "owner"::text as "owner",
       "principal_investigator"::text as "principal_investigator",
       "registered_identifier"::text as "registered_identifier",
       "title"::text as "title",
       "data_modality"::text as "data_modality",
       "source_datarepo_row_ids"::text as "source_datarepo_row_ids"
    from "dbt"."main"."anvil_dataset"
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*,
from source