{{ config(materialized='table', schema='alscompute_data') }}

with assertion_code as (
    select
    sample_id,
    NULL::text as "code",
    from (select distinct sample_id from {{ ref('alscompute_stg_sample') }})
    )

select 
  {{ generate_global_id(prefix='sa',descriptor=['sample_id','code'], study_id='alscompute') }}::text as "subjectassertion_id",
  sample_id::text as "external_id"
from assertion_code