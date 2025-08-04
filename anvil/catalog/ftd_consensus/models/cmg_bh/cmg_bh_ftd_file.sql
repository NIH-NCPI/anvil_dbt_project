{{ config(materialized='table', schema='cmg_bh_data') }}

{%- set relation = ref('cmg_bh_stg_sample') -%}
{%- set constant_columns = ['ftd_index','sample_id','subject_id','ingest_provenance','Submission_Batch','dbgap_sample_id','sample_provider','sample_source','tissue_affected_status'] -%}
{%- set sample_columns = get_columns(relation=relation, exclude=constant_columns) -%}

with 
unpivot_df as (
    {%- for col in sample_columns -%}
        select
            {{ constant_columns | join(', ') }},
            '{{ col }}' as "file_type",
            cast({{ col }} as varchar) as "drs_uri"
        from {{ ref('cmg_bh_stg_sample') }}
        where {{ col }} IS NOT NULL
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
)
select 
  NULL::text as "filename", -- TODO
  NULL::text as "format",
  NULL::text as "data_type",
  NULL::integer as "size",
  drs_uri::text as "drs_uri",
  {{ generate_global_id(prefix='fm',descriptor=['drs_uri'], study_id='cmg_bh') }}::text as "file_metadata", -- TODO ensure single quotes aren't strings
  {{ generate_global_id(prefix='ap',descriptor=['ingest_provenance'], study_id='cmg_bh') }}::text as "has_access_policy",
  {{ generate_global_id(prefix='fl',descriptor=['drs_uri'], study_id='cmg_bh') }}::text as "id" -- TODO drs_uri or filename?
from unpivot_df

    