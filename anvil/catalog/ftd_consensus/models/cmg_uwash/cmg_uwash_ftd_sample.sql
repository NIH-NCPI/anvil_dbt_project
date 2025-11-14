{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
 NULL::text as "parent_sample",
 coalesce(sm.sample_source, st.curie)::text as "sample_type",
 NULL::text as "availablity_status",
 NULL::text as "quantity_number",
 NULL::text as "quantity_units",
    {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='phs000693') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='sm',descriptor=['subject_id','sample_id'], study_id='phs000693') }}::text as "id",
    {{ generate_global_id(prefix='sb',descriptor=['subject_id'], study_id='phs000693') }}::text as "subject_id",
 NULL::text as "biospecimen_collection_id"
from {{ ref('cmg_uwash_stg_sample') }} as sm
left join {{ ref('cmg_uwash_stg_sequencing') }} as seq
using(sample_id)
left join {{ ref('sm_sample_type') }} as st
on seq.analyte_type = st.src_format
