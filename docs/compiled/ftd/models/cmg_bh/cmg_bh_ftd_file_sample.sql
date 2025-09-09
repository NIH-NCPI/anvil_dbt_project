with 
unpivot_df as (
)
select 
  'fl' || '_' || md5('cmg_bh' || '|' || cast(coalesce(drs_uri, '') as text))::text as "file_id",
  'sm' || '_' || md5('cmg_bh' || '|' || cast(coalesce(sample_id, '') as text))::text as "sample_id"
from unpivot_df