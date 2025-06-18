{{ config(materialized='table', schema='GREGoR_R01_HMB_20240208_data') }}

    with source as (
        select 
        participant_id AS other_family_member,
        twin_id AS participant_id,
        'KIN:009' AS relationship_code,
        {{ generate_global_id(prefix='ap',descriptor=['participant.consent_code'], study_id='GREGoR_R01_HMB_20240208') }}::text AS "has_access_policy",
        {{ generate_global_id(prefix='sb',descriptor=['participant.maternal_id'], study_id='GREGoR_R01_HMB_20240208') }}::text AS "family_member"
        from {{ ref('GREGoR_R01_HMB_20240208_stg_participant') }} as participant WHERE twin_id != '0'
        
        UNION 
        
        select 
        participant_id AS other_family_member,
        paternal_id AS participant_id,
        'KIN:028' AS relationship_code,
        {{ generate_global_id(prefix='ap',descriptor=['participant.consent_code'], study_id='GREGoR_R01_HMB_20240208') }}::text AS "has_access_policy",
        {{ generate_global_id(prefix='sb',descriptor=['participant.maternal_id'], study_id='GREGoR_R01_HMB_20240208') }}::text AS "family_member"
        from {{ ref('GREGoR_R01_HMB_20240208_stg_participant') }} as participant WHERE paternal_id != '0'
        
        UNION 
        
        select 
        participant_id AS other_family_member,
        maternal_id AS participant_id,
        'KIN:027' AS relationship_code,
        {{ generate_global_id(prefix='ap',descriptor=['participant.consent_code'], study_id='GREGoR_R01_HMB_20240208') }}::text AS "has_access_policy",
        {{ generate_global_id(prefix='sb',descriptor=['participant.maternal_id'], study_id='GREGoR_R01_HMB_20240208') }}::text AS "family_member"
        from {{ ref('GREGoR_R01_HMB_20240208_stg_participant') }} as participant WHERE maternal_id != '0'
        
        UNION 
        
        SELECT
        (select DISTINCT CAST(participant_id as STRING) FROM {{ ref('GREGoR_R01_HMB_20240208_stg_participant') }} as stg WHERE CAST(stg.family_id as STRING) = CAST(participant.family_id as STRING) AND CAST(stg.proband_relationship AS STRING) = 'Self') as other_family_member,
        participant.participant_id AS participant_id,
       CASE 
           WHEN participant.proband_relationship =  'Mother' THEN 'KIN:027' -- system: http://purl.org/ga4gh/kin.owl
           WHEN participant.proband_relationship =  'Father' THEN 'KIN:028'
           WHEN participant.proband_relationship =  'Sibling' THEN 'KIN:007'
           WHEN participant.proband_relationship =  'Child' THEN 'KIN:032'
           WHEN participant.proband_relationship =  'Maternal Half Sibling' THEN 'KIN:054'
           WHEN participant.proband_relationship =  'Paternal Half Sibling' THEN 'KIN:055'
           WHEN participant.proband_relationship =  'Maternal Grandmother' THEN 'KIN:052'
           WHEN participant.proband_relationship =  'Maternal Grandfather' THEN 'KIN:052'
           WHEN participant.proband_relationship =  'Paternal Grandmother' THEN 'KIN:053'
           WHEN participant.proband_relationship =  'Paternal Grandfather' THEN 'KIN:053'
           WHEN participant.proband_relationship =  'Maternal Aunt' THEN 'KIN:060'
           WHEN participant.proband_relationship =  'Paternal Aunt' THEN 'KIN:061'
           WHEN participant.proband_relationship =  'Maternal Uncle' THEN 'KIN:058'
           WHEN participant.proband_relationship =  'Paternal Uncle' THEN 'KIN:059'
           WHEN participant.proband_relationship =  'Niece' THEN 'KIN:001' -- The code is for 'relative of'
           WHEN participant.proband_relationship =  'Nephew' THEN 'KIN:001'
           WHEN participant.proband_relationship =  'Maternal 1st Cousin' THEN 'KIN:015'
           WHEN participant.proband_relationship =  'Paternal 1st Cousin' THEN 'KIN:016'
           WHEN participant.proband_relationship =  'Other' THEN 'HL7REL:OTH' -- system: http://terminology.hl7.org/CodeSystem/v2-0063
        END::text as "relationship_code",       
        {{ generate_global_id(prefix='ap',descriptor=['participant.consent_code'], study_id='GREGoR_R01_HMB_20240208') }}::text as "has_access_policy",
        {{ generate_global_id(prefix='sb',descriptor=['participant.participant_id'], study_id='GREGoR_R01_HMB_20240208') }}::text as "family_member"
        from {{ ref('GREGoR_R01_HMB_20240208_stg_participant') }} as participant
--         join { { ref('GREGoR_R01_HMB_20240208_stg_phenotype') }} as phenotype
-- on participant.participant_id = phenotype.participant_id 
        where participant.proband_relationship != 'Self'
    )

    select DISTINCT
        {{ generate_global_id(prefix='fm',descriptor=['participant_id','source.other_family_member'],study_id='GREGoR_R01_HMB_20240208') }}::text as "id",
        relationship_code, 
        family_member, 
        {{ generate_global_id(prefix='sb',descriptor=['source.other_family_member'], study_id='GREGoR_R01_HMB_20240208') }}::text as "other_family_member",
    from source
    
