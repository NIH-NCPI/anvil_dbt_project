{{ config(materialized='table', schema='alscompute_data') }}

{%- set fi_metadata_columns = ['crc32c','md5_hash'] -%}
{%- set sam_metadata_columns = ['reference_genome_build','sequencing_platform'] -%}

with
unpivot_df as (
    {%- for col in fi_metadata_columns -%}
        select
            distinct 
            file_id as "file_id",
            '{{ col }}' as "display",
            cast({{ col }} as varchar) as "value_display",
        from {{ ref('alscompute_stg_file_inventory') }}
        where {{ col }} IS NOT NULL
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
    
        union all
   
    {% for col in sam_metadata_columns %}
        select
            distinct 
            sample_id as "file_id",
            '{{ col }}' as "display",
            cast({{ col }} as varchar) as "value_display",
        from {{ ref('alscompute_stg_sample') }}
        where {{ col }} IS NOT NULL
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select 
NULL::text as "code",
display,
NULL::text as "value_code",
value_display,
    {{ generate_global_id(prefix='fd',descriptor=['file_id'], study_id='alscompute') }}::text as "id"
from unpivot_df