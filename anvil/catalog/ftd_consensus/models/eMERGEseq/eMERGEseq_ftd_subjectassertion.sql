{{ config(materialized='table', schema='eMERGEseq_data') }}

{% set relation = ref('eMERGEseq_stg_phecode') %}
{% set constant_columns = ['subject_id, ftd_index'] %}
{% set condition_columns = get_columns(relation=relation, exclude=constant_columns) %}  
with lookup as(
    select 
        variable_name as code, 
        description as display
    from {{ ref('phecode_seed') }}),

     
unpivot_df as (

    {% for col in condition_columns %}
        select
            {{ constant_columns | join(', ') }},
            "{{ col }}" as condition,
            "{{ col }}" as code,
            case when "{{ col }}" = 0 then 'LA9634-2'
                 when "{{ col }}" = 1 then 'LA9633-4'
            end as value_code,
            case when "{{ col }}" = 0 then 'Absent'
                 when "{{ col }}" = 1 then 'Present'
            end as value_display,
--             { { generate_global_id(prefix='sa', descriptor=['col'], study_id='eMERGEseq') }}::text as "id"
        from {{ ref('eMERGEseq_stg_phecode') }} as p
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

unpivoted_with_display as (
    select
        up.*,
        lookup.display as 'display'
    from unpivot_df as up
    left join lookup
        on up.condition = lookup.code
),

source as (
    select 
        'ehr_billing_code'::text as "assertion_type",
        -- GEN_UNKNOWN.age_at_assertion::text as "age_at_assertion",
        -- GEN_UNKNOWN.age_at_event::text as "age_at_event", 
        -- GEN_UNKNOWN.age_at_resolution::text as "age_at_resolution",
        -- GEN_UNKNOWN.value_number::text as "value_number",
        -- GEN_UNKNOWN.value_units::text as "value_units",
        -- GEN_UNKNOWN.value_units_display::text as "value_units_display",
        {{ generate_global_id(prefix='ap', descriptor=['subjectconsent.consent'], study_id='eMERGEseq') }}::text as "has_access_policy",
        {{ generate_global_id(prefix='sb', descriptor=['phecode.subject_id'], study_id='eMERGEseq') }}::text as "subject_id"
        
    from {{ ref('eMERGEseq_stg_phecode') }} as phecode
    join {{ ref('eMERGEseq_stg_subjectconsent') }} as subjectconsent
        on phecode.subject_id = subjectconsent.subject_id
)

select * from unpivoted_with_display
-- select * 
-- from source
-- join unpivoted_with_display on source.subject_id = unpivoted_with_display.subject_id
