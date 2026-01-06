{{ config(materialized='table', schema='cser_kidscanseq_data') }}

select 
NULL::text as "disease_limitation",
ap.consent_description::text as "description",
NULL::text as "website",
{{ generate_global_id(prefix='ap',descriptor=['s.consent_id'], study_id='cser_kidscanseq') }}::text as "id"
from (select distinct consent_id from {{ ref('cser_kidscanseq_stg_subject') }}) as s
left join {{ ref('ap_access_policy') }} as ap
on lower(s.consent_id) = lower(ap.consent_code)