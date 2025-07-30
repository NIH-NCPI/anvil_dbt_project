{{ config(materialized='table', schema='eMERGEseq_data') }}

{% set relation = ref('eMERGEseq_stg_phecode') %}
{% set constant_columns = ['subject_id', 'ftd_index'] %}
{% set condition_columns = get_columns(relation=relation, exclude=constant_columns) %}  
with lookup as(
    select 
        LOWER(variable_name) as code, 
    from {{ ref('phecode_seed') }}),

     
unpivot_df as (

    {% for col in condition_columns %}
        select
            {{ constant_columns | join(', ') }},
            '{{ col }}' as code,
        from {{ ref('eMERGEseq_stg_phecode') }} as p
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

source as (
    select
    {{ generate_global_id(prefix='sa',descriptor=['up.subject_id', 'up.code'], study_id='phs001616') }}::text as "subjectassertion_id",
       up.code::text as "external_id"
        from unpivot_df as up 
    )

    select 
        * 
    from source
    