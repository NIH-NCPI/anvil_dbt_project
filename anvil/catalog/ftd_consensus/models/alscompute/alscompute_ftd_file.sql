{{ config(materialized='table', schema='alscompute_data') }}

select 
name::text as "filename",
curie::text as "format",
NULL::text as "data_type",
size_in_bytes::text as "size",
file_ref::text as "drs_uri",
    {{ generate_global_id(prefix='fd',descriptor=['name'], study_id='alscompute') }}::text as "file_metadata",
    {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='alscompute') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='fi',descriptor=['name'], study_id='alscompute') }}::text as "id"
from (select distinct name, full_extension, size_in_bytes, file_ref, consent_id from {{ ref('alscompute_stg_file_inventory') }}) as fi
left join {{ ref('fl_format') }} as ff
on lower(replace(fi.full_extension,'.','')) = lower(ff.src_format)