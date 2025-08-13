{{ config(materialized='table', schema='GREGoR_R03_GRU_20250612_data') }}

    with code_cte as (
        select
        phenotype.participant_id,
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
        ),
        source as (
        select 
        NULL as "assertion_type",
        NULL as "age_at_assertion",
        phenotype.onset_age_range::text as "age_at_event",
        NULL as "age_at_resolution",
       code_cte.code,
        NULL as "display",
        CASE participant.affected_status
            WHEN 'Affected' THEN 'SCTID:782964007'
            ELSE null
        END::text as "value_code",
        phenotype.presence::text as "value_display",
        participant.age_at_enrollment::text as "value_number",
        NULL as "value_units",
        NULL as "value_units_display",
        {{ generate_global_id(prefix='ap',descriptor=['participant.consent_code'], study_id='phs003047') }}::text as "has_access_policy",
       {{ generate_global_id(prefix='sa',descriptor=['participant.participant_id', 'code_cte.code'], study_id='phs003047') }}::text as "id",
       {{ generate_global_id(prefix='sb',descriptor=['phenotype.participant_id'], study_id='phs003047') }}::text as "Subject_id"
        from {{ ref('GREGoR_R03_GRU_20250612_stg_participant') }} as participant
        join {{ ref('GREGoR_R03_GRU_20250612_stg_phenotype') }} as phenotype
on participant.participant_id = phenotype.participant_id 
        join code_cte
         on phenotype.participant_id = code_cte.participant_id
    )
    select 
        * 
    from source