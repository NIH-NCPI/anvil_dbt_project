{{ config(materialized='table', schema='eMERGEseq_data') }}
    {% set relation = ref('eMERGEseq_stg_pedigree') %}
    {% set constant_columns = ['ftd_index', 'sex', 'subject_id', 'family_id'] %}
    {% set pivot_columns = get_columns(relation=relation, exclude=constant_columns) %}  
    
    with 
    unpivot_df as (

        {% for col in pivot_columns %}
            select
                ftd_index, family_id,
                {{ col }} as "subject_id",
                subject_id as "orig_subject_id",
                NULL as "sex",
                '{{ col }}' as "family_role",
            from {{ ref('eMERGEseq_stg_pedigree') }} as p
            WHERE {{ col }} IS NOT NULL 
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
    ),
    
   all_pedigree as (
   select
        ftd_index,
        family_id,
        subject_id,
        orig_subject_id,
        p.sex,
        family_role,
    from  unpivot_df as u
    join {{ ref('eMERGEseq_stg_pedigree') }} as p
    on u.subject_id = p.subject_id
 union all
    select
        ftd_index,
        family_id,
        subject_id,
        NULL as "orig_subject_id",
        sex,
        'proband' as "family_role",
    from {{ ref('eMERGEseq_stg_pedigree') }} as p
    where mother_id is null
    and father_id is null
    and mz_twin_id is null
),
        source as(
        select 
        {{ generate_global_id(prefix='fm',descriptor=['pedigree.subject_id'], study_id='phs001616') }}::text as "family_member",
       CASE 
            WHEN pedigree.subject_id = pedigree.mother THEN 'MTH'
            WHEN pedigree.subject_id = pedigree.father THEN 'FTH'
            ELSE 'CHILD'
       END::text as "family_role",
       {{ generate_global_id(prefix='ap',descriptor=['subjectconsent.consent'], study_id='phs001616') }}::text as "has_access_policy",
       {{ generate_global_id(prefix='fm',descriptor=['pedigree.subject_id', 'pedigree.family_id'], study_id='phs001616') }}::text as "id",
       {{ generate_global_id(prefix='fm',descriptor=['pedigree.family_id'], study_id='phs001616') }}::text as "family_id"
       from {{ ref('eMERGEseq_stg_pedigree') }} as pedigree
       left join {{ ref('eMERGEseq_stg_subjectconsent') }} as subjectconsent
        on pedigree.subject_id = subjectconsent.subject_id
    )

    select 
        * 
    from source
    