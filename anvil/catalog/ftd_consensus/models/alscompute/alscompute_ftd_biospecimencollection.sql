{{ config(materialized='table', schema='alscompute_data') }}

with split_lowered_age as (
select  
    UNNEST(SPLIT(lower(age_of_collection_years), ',')) as split_age,
     sample_id,
     consent_id
    from (select distinct age_of_collection_years, sample_id, consent_id from{{ ref('alscompute_stg_sample') }}) as s
)

select 
CASE
    WHEN {{ remove_parenthetical_text('split_age') }} = '90 or older' THEN '90'
    ELSE {{ remove_parenthetical_text('split_age') }}
END as "age_at_collection",
NULL::text as "method",
NULL::text as "site",
NULL::text as "spatial_qualifier",
NULL::text as "laterality",
    {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='alscompute') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='bc',descriptor=['sample_id'], study_id='alscompute') }}::text as "id"
from split_lowered_age as sla