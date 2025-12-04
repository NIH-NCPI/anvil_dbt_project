{{ config(materialized='table', schema='cser_data') }}

select distinct
NULL::text as "disease_limitation",
'test'::text as "description",
NULL::text as "website",
{{ generate_global_id(prefix='ap',descriptor=['ingest_provenance'], study_id='cser') }}::text as "id"
from {{ ref('cser_stg_subject') }} as subject
-- left join {{ ref('ap_access_policy') }} as ap
-- on REPLACE(REPLACE(REPLACE(UPPER(subject.ingest_provenance), 'subject_AnVIL_CSER_SouthSeq_', ''),'.TSV',''),'_','-') = ap.consent_code
