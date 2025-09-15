with 
unpivot_df as (
)
select 
  'fl' || '_' || md5('cmg_bh' || '|' || cast(coalesce(drs_uri, '') as text))::text as "file_id",
  'sm' || '_' || md5('cmg_bh' || '|' || cast(coalesce(subject_id, '') as text))::text as "subject_id"
from unpivot_df