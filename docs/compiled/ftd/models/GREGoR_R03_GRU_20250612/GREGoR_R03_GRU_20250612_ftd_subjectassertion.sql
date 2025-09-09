

    with code_cte as (
        select DISTINCT
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
        select DISTINCT
        NULL as "assertion_type",
        NULL as "age_at_assertion",
        phenotype.onset_age_range::text as "age_at_event",
        NULL as "age_at_resolution",
        code_cte.code,
        NULL as "display",
        CASE 
            WHEN LOWER(participant.affected_status) = 'affected' THEN 'LA9633-4'
            WHEN LOWER(participant.affected_status) = 'unaffected' THEN 'LA9634-2'
            WHEN LOWER(participant.affected_status) = 'unknown' THEN 'LA4489-6'
            WHEN LOWER(participant.affected_status) = 'possibly affected' THEN 'LA15097-1'
            WHEN participant.affected_status is null THEN 'LA4489-6'
            ELSE CONCAT('FTD_FLAG: unhandled value_code: ', affected_status)
        END::text as "value_code",
        CASE 
            WHEN LOWER(participant.affected_status) = 'unaffected' THEN 'Absent'
            WHEN LOWER(participant.affected_status) = 'affected' THEN 'Present'
            WHEN LOWER(participant.affected_status) = 'unknown' THEN 'Unknown'
            WHEN LOWER(participant.affected_status) = 'possibly affected' THEN 'Possible'
            WHEN participant.affected_status IS NULL THEN 'Unknown'
            ELSE CONCAT('FTD_FLAG: unhandled value_display: ', presence)
        END::text as "value_display",
        participant.age_at_enrollment::text as "value_number",
        NULL as "value_units",
        NULL as "value_units_display",
        'ap' || '_' || md5('phs003047' || '|' || cast(coalesce(participant.consent_code, '') as text))::text as "has_access_policy",
       'sa' || '_' || md5('phs003047' || '|' || cast(coalesce(participant.participant_id, '') as text) || '|' || 'phs003047' || '|' || cast(coalesce(code_cte.code, '') as text))::text as "id",
       'sb' || '_' || md5('phs003047' || '|' || cast(coalesce(phenotype.participant_id, '') as text))::text as "Subject_id"
        from "dbt"."main_main"."GREGoR_R03_GRU_20250612_stg_participant" as participant
        left join "dbt"."main_main"."GREGoR_R03_GRU_20250612_stg_phenotype" as phenotype
on participant.participant_id = phenotype.participant_id 
        left join code_cte
         on phenotype.participant_id = code_cte.participant_id
    )
    select 
        * 
    from source