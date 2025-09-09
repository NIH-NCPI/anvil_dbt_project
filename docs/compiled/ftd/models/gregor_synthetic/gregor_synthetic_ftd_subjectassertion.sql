

    with source as (
        select 
        -- GEN_UNKNOWN.assertion_type::text as "assertion_type",
        -- GEN_UNKNOWN.age_at_assertion::text as "age_at_assertion",
        phenotype.onset_age_range::text as "age_at_event",
        -- GEN_UNKNOWN.age_at_resolution::text as "age_at_resolution",
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
        -- GEN_UNKNOWN.display::text as "display",
        CASE participant.affected_status
            WHEN 'Affected' THEN 'SCTID:782964007'
            ELSE null
        END::text as "value_code",
        phenotype.presence::text as "value_display",
        participant.age_at_enrollment::text as "value_number",
        -- GEN_UNKNOWN.value_units::text as "value_units",
        -- GEN_UNKNOWN.value_units_display ::text as "value_units_display",
        -- GEN_UNKNOWN.has_access_policy::text as "has_access_policy",
        'ap' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(participant.consent_code, '') as text))::text as "has_access_policy",
       'sa' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(participant.participant_id, '') as text))::text as "id",
       'sb' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(phenotype.participant_id, '') as text))::text as "Subject_id"
        from "dbt"."main_main"."gregor_synthetic_stg_participant" as participant
        join "dbt"."main_main"."gregor_synthetic_stg_phenotype" as phenotype
on participant.participant_id = phenotype.participant_id 
    )
    select 
        * 
    from source

    -- ,
    -- ap as (
    --     select
    --     disease_limitation, id
    --     from "dbt"."main_gregor_synthetic_data"."gregor_synthetic_ftd_accesspolicy"
    -- )

    -- select 
    --     source.*,
    --     ap.id::text as "has_access_policy"
    -- from source
    -- left join ap 
    -- on source.value_code = ap.disease_limitation