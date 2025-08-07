{{ config(materialized='table', schema='eMERGEseq') }}

    with source as (
        select 
        subject_id AS other_family_member,
        twin_id AS subject_id,
        'KIN:009' AS relationship_code,
        {{ generate_global_id(prefix='ap',descriptor=['subjectconsent.consent'], study_id='phs001616') }}::text AS "has_access_policy",
        from {{ ref('eMERGEseq_stg_pedigree') }} as pedigree
        join {{ ref('eMERGEseq_stg_subjectconsent') }}  as subjectconsent 
        on pedigree.subject_id = subjectconsent.subject_id
        WHERE twin_id != 'NA'

        UNION 

        select 
        subject_id AS other_family_member, 
        father AS subject_id,
        'KIN:028' AS relationship_code,
        {{ generate_global_id(prefix='ap',descriptor=['subjectconsent.consent'], study_id='phs001616') }}::text AS "has_access_policy",
        from {{ ref('eMERGEseq_stg_pedigree') }} as pedigree
        join {{ ref('eMERGEseq_stg_subjectconsent') }}  as subjectconsent 
        on pedigree.subject_id = subjectconsent.subject_id
        WHERE father != 'NA'

        UNION 

        select 
        subject_id AS other_family_member, 
        mother AS subject_id,
        'KIN:027' AS relationship_code,
        {{ generate_global_id(prefix='ap',descriptor=['subjectconsent.consent'], study_id='phs001616') }}::text AS "has_access_policy",
        from {{ ref('eMERGEseq_stg_pedigree') }} as pedigree
        join {{ ref('eMERGEseq_stg_subjectconsent') }}  as subjectconsent 
        on pedigree.subject_id = subjectconsent.subject_id
        WHERE mother != 'NA'
    )

    select DISTINCT
        {{ generate_global_id(prefix='fm',descriptor=['subject_id','source.other_family_member'],study_id='phs001616') }}::text as "id",
        relationship_code, 
        {{ generate_global_id(prefix='sb',descriptor=['source.subject_id'], study_id='phs001616') }}::text AS "family_member",
        {{ generate_global_id(prefix='sb',descriptor=['source.other_family_member'], study_id='phs001616') }}::text as "other_family_member"
    from source
 