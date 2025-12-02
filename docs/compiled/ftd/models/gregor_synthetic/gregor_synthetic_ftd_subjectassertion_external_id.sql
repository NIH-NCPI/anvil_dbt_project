

 with source as (
        select 
        participant_id,
        REPLACE(term_id, '?', '')::text as "code",
        from "dbt"."main_main"."gregor_synthetic_stg_participant" as participant
        join "dbt"."main_main"."gregor_synthetic_stg_phenotype" as phenotype
        using(participant_id) 
    )

    select distinct
    'sa' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(participant_id, '') as text) || '|' || 'gregor_synthetic' || '|' || cast(coalesce(code, '') as text))::text as "SubjectAssertion_id",
    participant_id::text as "external_id"
    from source as s
    left join "dbt"."main"."gregor_synthetic_annotations" as ga
    on lower(s.code) = lower(ga.searched_code)