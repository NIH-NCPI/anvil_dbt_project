{{ config(materialized='table', schema='cmg_yale_data') }}
{%- set fi_relation = ref('cmg_yale_stg_file_inventory') -%}
{%- set fi_pivot_columns = ['crc32c','md5_hash'] -%}
{%- set fi_constant_columns = get_columns(relation=fi_relation, exclude=fi_pivot_columns) -%}
{%- set seq_relation = ref('cmg_yale_stg_sequencing') -%}
{%- set seq_constant_columns = ['ftd_index','sequencing_id','date_data_generation','sample_id','seq_filename','ingest_provenance','sequencing_id_fileref','capture_region_bed_file','exome_capture_platform'] -%}
{%- set seq_pivot_columns = get_columns(relation=seq_relation, exclude=seq_constant_columns) -%}

with
unpivot_df as (
    {%- for col in fi_pivot_columns -%}
        select
            distinct 
            file_id as "filename",
            '{{ col }}' as "code",
            cast({{ col }} as varchar) as "value_code"
        from {{ ref('cmg_yale_stg_file_inventory') }}
        where {{ col }} IS NOT NULL
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
        union all
   
    {% for col in seq_pivot_columns %}
        select
            distinct 
            sequencing_id as "filename",
            '{{ col }}' as "code",
            cast({{ col }} as varchar) as "value_code"
        from {{ ref('cmg_yale_stg_sequencing') }}
        where {{ col }} IS NOT NULL
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select 
  distinct
  code,
  code::text as "display", -- TODO Join dd seed
  value_code::text as "value_code",
  value_code::text as "value_display",
  {{ generate_global_id(prefix='fd',descriptor=['filename'], study_id='cmg_yale') }}::text as "id"
from unpivot_df
