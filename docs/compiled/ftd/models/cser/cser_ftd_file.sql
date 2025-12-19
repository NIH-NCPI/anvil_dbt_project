

select distinct
name::text as "filename",
curie::text as "format",
NULL::text as "data_type",
size_in_bytes::text as "size",
file_ref::text as "drs_uri",
    'fd' || '_' || md5('cser' || '|' || cast(coalesce(name, '') as text))::text as "file_metadata",
    'ap' || '_' || md5('cser' || '|' || cast(coalesce(consent_id, '') as text))::text as "has_access_policy",
    'fl' || '_' || md5('cser' || '|' || cast(coalesce(file_id, '') as text))::text as "id"
from "dbt"."main_main"."cser_stg_file_inventory" as fi
left join "dbt"."main"."fl_format" as ff
on lower(replace(fi.full_extension,'.','')) = lower(ff.src_format)