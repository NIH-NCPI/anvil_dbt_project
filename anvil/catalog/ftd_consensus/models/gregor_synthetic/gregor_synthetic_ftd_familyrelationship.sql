{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
        {{ generate_global_id(prefix='',descriptor=[''], study_id='gregor_synthetic') }}::text as "family_member",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='gregor_synthetic') }}::text as "other_family_member",
        CASE participant.proband_relationship
            WHEN 'Mother' THEN 'MTH'
            WHEN 'Father' THEN 'FTH'
            WHEN 'Sibling' THEN 'SIB'
            WHEN 'Child' THEN 'CHILD'
            WHEN 'Maternal Half Sibling' THEN 'HSIB'
            WHEN 'Paternal Half Sibling' THEN 'HSIB'
            WHEN 'Maternal Grandmother' THEN 'MGRMTH'
            WHEN 'Maternal Grandfather' THEN 'MGRFTH'
            WHEN 'Paternal Grandmother' THEN 'PGRMTH'
            WHEN 'Paternal Grandfather' THEN 'PGRFTH'
            WHEN 'Maternal Aunt' THEN 'MAUNT'
            WHEN 'Paternal Aunt' THEN 'PAUNT'
            WHEN 'Maternal Uncle' THEN 'MUNCLE'
            WHEN 'Paternal Uncle' THEN 'PUNCLE'
            WHEN 'Niece' THEN 'NIECE'
            WHEN 'Nephew' THEN 'NEPHEW'
            WHEN 'Maternal 1st Cousin' THEN 'MCOUSN'
            WHEN 'Paternal 1st Cousin' THEN 'PCOUSN'
            ELSE participant.proband_relationship
        END::text as "relationship_code",       {{ generate_global_id(prefix='',descriptor=[''], study_id='gregor_synthetic') }}::text as "has_access_policy",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='gregor_synthetic') }}::text as "id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype
on participant.anvil_gregor_gss_u07_gru_participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
    