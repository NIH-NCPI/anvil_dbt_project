{{ config(materialized='table', schema='cmg_yale_data') }}
{%- set pivot_columns = ['crai','cram','seq_filename','sequencing_id_fileref']

with
combo_df as (
  select 
    distinct
    consent_id,
    crai,
    cram,
    seq_filename,
    sequencing_id_fileref
  from 
    (select distinct crai, cram from {{ ref('cmg_yale_stg_sample') }}
     full join
     select distinct seq_filename, sequencing_id_fileref from {{ ref('cmg_yale_stg_sequencing') }}
     using (subject_id, consent_id)
)
,unpivot_df as (
    {%- for col in pivot_columns -%}
        select
            distinct 
            consent_id,
            '{{ col }}' as "file_type",
            cast({{ col }} as varchar) as "drs_uri"
        from combo_df
        where {{ col }} IS NOT NULL
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select 
  drs_uri::text as "filename",
  NULL::text as "format",
  file_type::text as "data_type",
  NULL::integer as "size",
  drs_uri::text as "drs_uri",
  {{ generate_global_id(prefix='fm',descriptor=['drs_uri'], study_id='cmg_yale') }}::text as "file_metadata",
  {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='cmg_yale') }}::text as "has_access_policy",
  {{ generate_global_id(prefix='fl',descriptor=['drs_uri'], study_id='cmg_yale') }}::text as "id"
from unpivot_df