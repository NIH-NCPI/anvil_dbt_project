{{ config(materialized='table', schema='eMERGEseq_data') }}

    with source as (
     select 
     --     GEN_UNKNOWN.disease_limitation::text as "disease_limitation",
         'General Research Use' as "description",
     --    GEN_UNKNOWN.website::text as "website",
        {{ generate_global_id(prefix='ap',descriptor=['subjectconsent.subject_id'], study_id='eMERGEseq') }}::text as "id"
    from {{ ref('eMERGEseq_stg_subjectconsent') }} as subjectconsent
  WHERE subjectconsent.consent IN ('1', '3', '4', '5', '9')

    UNION
        
    select 
     --     GEN_UNKNOWN.disease_limitation::text as "disease_limitation",
        'IRB Approval Required' as "description",
     --    GEN_UNKNOWN.website::text as "website",
        {{ generate_global_id(prefix='ap',descriptor=['subjectconsent.subject_id'], study_id='eMERGEseq') }}::text as "id"
    from {{ ref('eMERGEseq_stg_subjectconsent') }} as subjectconsent
    WHERE subjectconsent.consent IN ('3', '4', '5', '10')

    UNION
        
    select 
     --     GEN_UNKNOWN.disease_limitation::text as "disease_limitation",
        'Not-for-profit use only' as "description",
     --    GEN_UNKNOWN.website::text as "website",
        {{ generate_global_id(prefix='ap',descriptor=['subjectconsent.subject_id'], study_id='eMERGEseq') }}::text as "id"
    from {{ ref('eMERGEseq_stg_subjectconsent') }} as subjectconsent
    WHERE subjectconsent.consent IN ('4', '5', '8', '9')
    )

     UNION
        
    select 
     --     GEN_UNKNOWN.disease_limitation::text as "disease_limitation",
        'Publication Required' as "description",
     --    GEN_UNKNOWN.website::text as "website",
        {{ generate_global_id(prefix='ap',descriptor=['subjectconsent.subject_id'], study_id='eMERGEseq') }}::text as "id"
    from {{ ref('eMERGEseq_stg_subjectconsent') }} as subjectconsent
    WHERE subjectconsent.consent IN ('5', '10')
    )
    
    UNION
        
    select 
     --     GEN_UNKNOWN.disease_limitation::text as "disease_limitation",
        'Health/Medical/Biomedical' as "description",
     --    GEN_UNKNOWN.website::text as "website",
        {{ generate_global_id(prefix='ap',descriptor=['subjectconsent.subject_id'], study_id='eMERGEseq') }}::text as "id"
    from {{ ref('eMERGEseq_stg_subjectconsent') }} as subjectconsent
    WHERE subjectconsent.consent IN ('6', '7', '8', '10')
    )

    UNION
        
    select 
     --     GEN_UNKNOWN.disease_limitation::text as "disease_limitation",
        'Genetic Studies only' as "description",
     --    GEN_UNKNOWN.website::text as "website",
        {{ generate_global_id(prefix='ap',descriptor=['subjectconsent.subject_id'], study_id='eMERGEseq') }}::text as "id"
    from {{ ref('eMERGEseq_stg_subjectconsent') }} as subjectconsent
    WHERE subjectconsent.consent IN ('7')
    )
    select 
        * 
    from source
    