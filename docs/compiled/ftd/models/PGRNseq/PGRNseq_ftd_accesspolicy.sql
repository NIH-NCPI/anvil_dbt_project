(
    select DISTINCT
        CASE consent
            WHEN '2' THEN 'Dementia'
            ELSE NULL 
        END as "disease_limitation",
        'Health/Medical/Biomedical' as "description",
        NULL as "website",
        'ap' || '_' || md5('phs000906' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "id"
    from "dbt"."main_main"."PGRNseq_stg_subjectconsent" as subjectconsent
      where subjectconsent.consent in ('1')
        )
    union all(
    select DISTINCT
        CASE consent
            WHEN '2' THEN 'Dementia'
            ELSE NULL 
        END as "disease_limitation",
        'Disease' as "description",
        NULL as "website",
        'ap' || '_' || md5('phs000906' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "id"
    from "dbt"."main_main"."PGRNseq_stg_subjectconsent" as subjectconsent
      where subjectconsent.consent in ('2')
        )
    union all(
    select DISTINCT
        CASE consent
            WHEN '2' THEN 'Dementia'
            ELSE NULL 
        END as "disease_limitation",
        'General Research Use' as "description",
        NULL as "website",
        'ap' || '_' || md5('phs000906' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "id"
    from "dbt"."main_main"."PGRNseq_stg_subjectconsent" as subjectconsent
      where subjectconsent.consent in ('3')
        )
    