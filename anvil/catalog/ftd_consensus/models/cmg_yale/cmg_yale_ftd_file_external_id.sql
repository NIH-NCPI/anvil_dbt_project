{{ config(materialized='table', schema='cmg_yale_data') }}
{%- set pivot_columns = ['crai','cram','seq_filename','sequencing_id_fileref'] -%}

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
    ((select distinct sample_id, consent_id, crai, cram from {{ ref('cmg_yale_stg_sample') }}) as s
     full join
     (select distinct sample_id, consent_id, seq_filename, sequencing_id_fileref from {{ ref('cmg_yale_stg_sequencing') }}) as seq
     using (sample_id, consent_id)
    ) as s
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
  {{ generate_global_id(prefix='fl',descriptor=['drs_uri'], study_id='cmg_yale') }}::text as "file_id",
  drs_uri::text as "external_id"
from unpivot_df