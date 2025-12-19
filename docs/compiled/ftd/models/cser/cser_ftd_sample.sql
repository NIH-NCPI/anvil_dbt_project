

select DISTINCT
  NULL::text as "parent_sample",
  st.curie::text as "sample_type",
  NULL::text as "availablity_status",
  NULL::text as "quantity_number",
  NULL::text as "quantity_units",
    'ap' || '_' || md5('cser' || '|' || cast(coalesce(consent_id, '') as text))::text as "has_access_policy",
    'sm' || '_' || md5('cser' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cser' || '|' || cast(coalesce(sample_id, '') as text))::text as "id",
    'sb' || '_' || md5('cser' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cser' || '|' || cast(coalesce(consent_id, '') as text))::text as "subject_id",
    NULL::text as "biospecimen_collection_id"
from "dbt"."main_main"."cser_stg_sample" as s
left join "dbt"."main"."sm_sample_type" as st
on lower(s.sample_source)= lower(st.src_format)