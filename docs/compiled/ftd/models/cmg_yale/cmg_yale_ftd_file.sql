

with 
get_consents as (
  select crai as file, consent_id 
  from "dbt"."main_main"."cmg_yale_stg_sample"
    union all 
  select cram as file, consent_id 
  from "dbt"."main_main"."cmg_yale_stg_sample"
    union all 
  select seq_filename as file, consent_id 
  from "dbt"."main_main"."cmg_yale_stg_sequencing"
    union all 
  select sequencing_id_fileref as file, consent_id 
  from "dbt"."main_main"."cmg_yale_stg_sequencing"
)

select 
 distinct 
 NULL as data_type,
 size_in_bytes as "size",
 consent_id,
 
 curie::text as "format",
 coalesce(full_extension, 'FTD_NULL') AS "ftd_format", -- flag nulls for analysis
 coalesce(uri, 'Needs Handling - nullable') AS "ftd_format", -- flag unhandled strings
 
 name as "filename",
 file_ref as "drs_uri",
 'fd' || '_' || md5('cmg_yale' || '|' || cast(coalesce(filename, '') as text))::text as "file_metadata",
 'ap' || '_' || md5('cmg_yale' || '|' || cast(coalesce(consent_id, '') as text))::text as "has_access_policy",
 'fl' || '_' || md5('cmg_yale' || '|' || cast(coalesce(filename, '') as text))::text as "id"
from 
  "dbt"."main_main"."cmg_yale_stg_file_inventory"
  left join get_consents 
    on file = file_ref
  left join "dbt"."main"."fl_format" as ff
    on lower(replace(full_extension,'.','')) = src_format