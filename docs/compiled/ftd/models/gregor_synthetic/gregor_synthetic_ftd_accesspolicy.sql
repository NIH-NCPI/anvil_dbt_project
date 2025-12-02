

    with source as (
        select distinct
        NULL::text as "disease_limitation",
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
        NULL::text as "website",
      'ap' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(participant.consent_code, '') as text))::text as "id"
        from "dbt"."main_main"."gregor_synthetic_stg_participant" as participant
    )

    select 
        * 
    from source