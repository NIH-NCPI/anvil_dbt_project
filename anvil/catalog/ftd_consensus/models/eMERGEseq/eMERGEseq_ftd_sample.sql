{{ config(materialized='table', schema='eMERGEseq_data') }}

    with source as (
        select 
--         { { generate_global_id(prefix='',descriptor=[''], study_id='eMERGEseq') }}::text as "parent_sample",
       CASE WHEN sampleattributes.analyte_type = 'DNA' THEN 'LNC:LP18329-0'
       END::text as "sample_type",
--        GEN_UNKNOWN.availablity_status::text as "availablity_status",
--        GEN_UNKNOWN.quantity_number::text as "quantity_number",
--        GEN_UNKNOWN.quantity_units::text as "quantity_units",
       {{ generate_global_id(prefix='ap',descriptor=['subjectconsent.consent'], study_id='phs001616') }}::text as "has_access_policy",
       {{ generate_global_id(prefix='sm',descriptor=['sampleattributes.sample_id'], study_id='phs001616') }}::text as "id",
       {{ generate_global_id(prefix='sb',descriptor=['samplesubjectmapping.subject_id'], study_id='phs001616') }}::text as "subject_id",
--        { { generate_global_id(prefix='',descriptor=[''], study_id='eMERGEseq') }}::text as "biospecimen_collection_id"
        from {{ ref('eMERGEseq_stg_sampleattributes') }} as sampleattributes
        join {{ ref('eMERGEseq_stg_samplesubjectmapping') }} as samplesubjectmapping
        on sampleattributes.sample_id = samplesubjectmapping.sample_id
        join {{ ref('eMERGEseq_stg_subjectconsent') }} as subjectconsent
        on samplesubjectmapping.subject_id = subjectconsent.subject_id
    )

    select 
        * 
    from source
    