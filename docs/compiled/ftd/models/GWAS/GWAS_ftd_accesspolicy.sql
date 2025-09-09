with source as ((
    select DISTINCT
        CASE consent
            WHEN '6' THEN 'Childhood Diseases'
            WHEN '7' THEN 'Dementia'
            ELSE NULL 
        END as "disease_limitation",
        'General Research Use' as "description",
        NULL as "website",
        'ap' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "id"
    from "dbt"."main_main"."GWAS_stg_subjectconsent" as subjectconsent
      where subjectconsent.consent in ('1', 
          '2', 
          '3', 
          '9')
        )
    union all(
    select DISTINCT
        CASE consent
            WHEN '6' THEN 'Childhood Diseases'
            WHEN '7' THEN 'Dementia'
            ELSE NULL 
        END as "disease_limitation",
        'IRB Approval Required' as "description",
        NULL as "website",
        'ap' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "id"
    from "dbt"."main_main"."GWAS_stg_subjectconsent" as subjectconsent
      where subjectconsent.consent in ('2', 
          '3', 
          '9')
        )
    union all(
    select DISTINCT
        CASE consent
            WHEN '6' THEN 'Childhood Diseases'
            WHEN '7' THEN 'Dementia'
            ELSE NULL 
        END as "disease_limitation",
        'Collaboration Required' as "description",
        NULL as "website",
        'ap' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "id"
    from "dbt"."main_main"."GWAS_stg_subjectconsent" as subjectconsent
      where subjectconsent.consent in ('3')
        )
    union all(
    select DISTINCT
        CASE consent
            WHEN '6' THEN 'Childhood Diseases'
            WHEN '7' THEN 'Dementia'
            ELSE NULL 
        END as "disease_limitation",
        'Not-for-profit use only' as "description",
        NULL as "website",
        'ap' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "id"
    from "dbt"."main_main"."GWAS_stg_subjectconsent" as subjectconsent
      where subjectconsent.consent in ('3')
        )
    union all(
    select DISTINCT
        CASE consent
            WHEN '6' THEN 'Childhood Diseases'
            WHEN '7' THEN 'Dementia'
            ELSE NULL 
        END as "disease_limitation",
        'Publication Required' as "description",
        NULL as "website",
        'ap' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "id"
    from "dbt"."main_main"."GWAS_stg_subjectconsent" as subjectconsent
      where subjectconsent.consent in ('2', 
          '8', 
          '9')
        )
    union all(
    select DISTINCT
        CASE consent
            WHEN '6' THEN 'Childhood Diseases'
            WHEN '7' THEN 'Dementia'
            ELSE NULL 
        END as "disease_limitation",
        'Health/Medical/Biomedical' as "description",
        NULL as "website",
        'ap' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "id"
    from "dbt"."main_main"."GWAS_stg_subjectconsent" as subjectconsent
      where subjectconsent.consent in ('4', 
          '5', 
          '8', 
          '10')
        )
    union all(
    select DISTINCT
        CASE consent
            WHEN '6' THEN 'Childhood Diseases'
            WHEN '7' THEN 'Dementia'
            ELSE NULL 
        END as "disease_limitation",
        'Genetic Studies only' as "description",
        NULL as "website",
        'ap' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "id"
    from "dbt"."main_main"."GWAS_stg_subjectconsent" as subjectconsent
      where subjectconsent.consent in ('2', 
          '8', 
          '10')
        )
    union all(
    select DISTINCT
        CASE consent
            WHEN '6' THEN 'Childhood Diseases'
            WHEN '7' THEN 'Dementia'
            ELSE NULL 
        END as "disease_limitation",
        'Methods' as "description",
        NULL as "website",
        'ap' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "id"
    from "dbt"."main_main"."GWAS_stg_subjectconsent" as subjectconsent
      where subjectconsent.consent in ('5')
        )
    union all(
    select DISTINCT
        CASE consent
            WHEN '6' THEN 'Childhood Diseases'
            WHEN '7' THEN 'Dementia'
            ELSE NULL 
        END as "disease_limitation",
        'Disease' as "description",
        NULL as "website",
        'ap' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "id"
    from "dbt"."main_main"."GWAS_stg_subjectconsent" as subjectconsent
      where subjectconsent.consent in ('6', 
          '7')
        )
    )
select * from source