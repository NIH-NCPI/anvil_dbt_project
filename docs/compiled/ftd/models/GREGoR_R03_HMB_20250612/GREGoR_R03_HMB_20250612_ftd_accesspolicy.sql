

    with source as (
        select 
    --     GEN_UNKNOWN.disease_limitation::text as "disease_limitation",
        CASE participant.consent_code
            WHEN 'GRU' THEN 'General Research Use'
            WHEN 'HMB' THEN 'Health/Medical/Biomedical'
            WHEN 'DS' THEN 'Disease-Specific (Disease/Trait/Exposure)'
            WHEN 'IRB' THEN 'IRB Approval Required'
            WHEN 'PUB' THEN 'Publication Required'
            WHEN 'COL' THEN 'Collaboration Required'
            WHEN 'NPU' THEN 'Not-for-profit use only'
            WHEN 'MDS' THEN 'Methods'
            WHEN 'GSO' THEN 'Genetic Studies only'
            WHEN 'GSR' THEN 'Genomic Summary Results'
        END::text as "description",    
        --    GEN_UNKNOWN.website::text as "website",
      'ap' || '_' || md5('GREGoR_R03_HMB_20250612' || '|' || cast(coalesce(participant.consent_code, '') as text))::text as "id"
        from "dbt"."main_main"."GREGoR_R03_HMB_20250612_stg_participant" as participant
        join "dbt"."main_main"."GREGoR_R03_HMB_20250612_stg_phenotype" as phenotype
on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source