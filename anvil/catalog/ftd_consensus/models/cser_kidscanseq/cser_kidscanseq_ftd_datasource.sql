{{ config(materialized='table', schema='cser_kidscanseq_data') }}

select 
'2fdba9a4-6593-439a-a7fc-c3a5825c26cd'::text as "snapshot_id",
'datarepo-0f2e95ad'::text as "google_data_project",
'ANVIL_CSER_KidsCanSeq_GRU_20221208_ANV5_202402292138'::text as "snapshot_dataset",
NULL::text as "table_id",
NULL::text as "parameterized_query",
    {{ generate_global_id(prefix='ds',descriptor=['registered_identifier'], study_id='cser_kidscanseq') }}::text as "id"
from (select distinct registered_identifier from {{ ref('cser_kidscanseq_stg_anvil_dataset') }}) as anvil_dataset