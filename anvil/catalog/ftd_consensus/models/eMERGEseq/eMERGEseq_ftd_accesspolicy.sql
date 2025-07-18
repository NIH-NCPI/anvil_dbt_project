{{ config(materialized='table', schema='eMERGEseq_data') }}

    with source as (
        select 
        GEN_UNKNOWN.disease_limitation::text as "disease_limitation",
       GEN_UNKNOWN.description::text as "description",
       GEN_UNKNOWN.website::text as "website",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='eMERGEseq') }}::text as "id"
        from {{ ref('eMERGEseq_stg_subjectconsent') }} as subjectconsent
        join {{ ref('eMERGEseq_stg_demographics') }} as demographics
on subjectconsent.subject_id = demographics.subject_id  join {{ ref('eMERGEseq_stg_phecode') }} as phecode
on   join {{ ref('eMERGEseq_stg_samplesubjectmapping') }} as samplesubjectmapping
on   join {{ ref('eMERGEseq_stg_sampleattributes') }} as sampleattributes
on  
    )

    select 
        * 
    from source
    