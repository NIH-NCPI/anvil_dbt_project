{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
        participant_id,
        consent_code,
        phenotype.onset_age_range::text as "age_at_event",
        CASE 
            WHEN UPPER(phenotype.ontology) = 'HPO' THEN 'HP'
            WHEN UPPER(phenotype.ontology) = 'MONDO' THEN 'MONDO'
            WHEN UPPER(phenotype.ontology) = 'MAXO' THEN 'MAXO'
            WHEN UPPER(phenotype.ontology) = 'NCIT' THEN 'NCIT'
            WHEN UPPER(phenotype.ontology) = 'SNOMED CT' THEN 'SNOMED'
            WHEN UPPER(phenotype.ontology) = 'SYMP' THEN 'SYMP'
            WHEN UPPER(phenotype.ontology) = 'LOINC' THEN 'LOINC'
            WHEN UPPER(phenotype.ontology) = 'MEDDRA' THEN 'MEDDRA'
            WHEN UPPER(phenotype.ontology) = 'MESH' THEN 'MESH'
            WHEN UPPER(phenotype.ontology) = 'UCUM' THEN 'UCUM'
            WHEN UPPER(phenotype.ontology) = 'OMIT' THEN 'OMIT'
            WHEN UPPER(phenotype.ontology) = 'OMIM' THEN 'MIM'
            WHEN UPPER(phenotype.ontology) = 'CDCREC' THEN 'CDCREC'
            ELSE phenotype.ontology
        END || ':' || SPLIT_PART(phenotype.term_id, ':', 2)::text as "code",
        CASE 
            WHEN LOWER(phenotype.presence) = 'present' THEN 'LA9633-4'
            WHEN LOWER(phenotype.presence) = 'absent' THEN 'LA9634-2'
            WHEN phenotype.presence is null THEN 'LA4489-6'
            ELSE CONCAT('FTD_FLAG: unhandled value_code: ', presence)
        END::text as "value_code",
        CASE 
            WHEN LOWER(phenotype.presence) = 'absent' THEN 'Absent'
            WHEN LOWER(phenotype.presence) = 'present' THEN 'Present'
            WHEN phenotype.presence IS NULL THEN 'Unknown'
            ELSE CONCAT('FTD_FLAG: unhandled value_display: ', presence)
        END::text as "value_display",
        participant.age_at_enrollment::text as "value_number"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype
        using(participant_id) 
    )

        select 
        NULL::text as "assertion_type",
        NULL::text as "age_at_assertion",
        age_at_event,
        NULL::text as "age_at_resolution",
        code,
        NULL::text as "display",
        value_code,
        value_display,
        value_number,
        NULL::text as "value_units",
        NULL::text as "value_units_display",
       {{ generate_global_id(prefix='ap',descriptor=['consent_code'], study_id='gregor_synthetic') }}::text as "has_access_policy",
       {{ generate_global_id(prefix='sb',descriptor=['participant_id', 'consent_code'], study_id='gregor_synthetic') }}::text as "Subject_id",
       {{ generate_global_id(prefix='sa',descriptor=['participant_id', 'code'], study_id='gregor_synthetic') }}::text as "id"
       
       from source