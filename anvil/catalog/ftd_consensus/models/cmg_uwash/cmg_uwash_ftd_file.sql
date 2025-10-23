{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
name::text as "filename",
curie::text as "format",
NULL::text as "data_type",
size_in_bytes::text as "size",
file_ref::text as "drs_uri",
{{ generate_global_id(prefix='fd',descriptor=['name'], study_id='phs000693') }}::text as "file_metadata",
{{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='phs000693') }}::text as "has_access_policy",
{{ generate_global_id(prefix='fl',descriptor=['name'], study_id='phs000693') }}::text as "id"
from {{ ref('cmg_uwash_stg_file_inventory') }} as fi
left join {{ ref('fl_format') }} as ff
on lower(replace(fi.full_extension,'.','')) = ff.src_format