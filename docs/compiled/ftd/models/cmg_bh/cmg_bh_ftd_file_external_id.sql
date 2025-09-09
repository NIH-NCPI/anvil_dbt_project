with 
unpivot_df as (
)
select 
  'fl' || '_' || md5('cmg_bh' || '|' || cast(coalesce(drs_uri, '') as text))::text as "file_id",
  drs_uri::text as "external_id"
from unpivot_df