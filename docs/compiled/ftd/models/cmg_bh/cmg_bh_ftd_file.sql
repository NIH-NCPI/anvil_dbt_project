with 
unpivot_df as (
)
select 
  drs_uri::text as "filename",
  NULL::text as "format",
  file_type::text as "data_type",
  NULL::integer as "size",
  drs_uri::text as "drs_uri",
  'fm' || '_' || md5('cmg_bh' || '|' || cast(coalesce(drs_uri, '') as text))::text as "file_metadata",
  'ap' || '_' || md5('cmg_bh' || '|' || cast(coalesce(ingest_provenance, '') as text))::text as "has_access_policy",
  'fl' || '_' || md5('cmg_bh' || '|' || cast(coalesce(drs_uri, '') as text))::text as "id"
from unpivot_df