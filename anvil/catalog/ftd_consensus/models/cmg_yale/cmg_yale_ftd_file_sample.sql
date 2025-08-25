{{ config(materialized='table', schema='cmg_yale_data') }}
{%- set pivot_columns = ['crai','cram'] -%}

with
combo_df as (
  select 
    distinct
    sample_id,
    crai,
    cram
  from 
    (select distinct sample_id, crai, cram from {{ ref('cmg_yale_stg_sample') }}) as s
)
,unpivot_df as (
    {%- for col in pivot_columns -%}
        select
            distinct 
            sample_id,
            '{{ col }}' as "file_type",
            cast({{ col }} as varchar) as "drs_uri"
        from combo_df
        where {{ col }} IS NOT NULL
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select 
  {{ generate_global_id(prefix='fl',descriptor=['drs_uri'], study_id='cmg_yale') }}::text as "file_id",
  {{ generate_global_id(prefix='sm',descriptor=['sample_id'], study_id='cmg_yale') }}::text as "sample_id"
from unpivot_df