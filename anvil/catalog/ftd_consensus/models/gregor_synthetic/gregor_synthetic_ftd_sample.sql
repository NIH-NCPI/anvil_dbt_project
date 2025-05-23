{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
        GEN_UNKNOWN.parent_sample::text as "parent_sample",
       GEN_UNKNOWN.sample_type::text as "sample_type",
       GEN_UNKNOWN.availablity_status::text as "availablity_status",
       GEN_UNKNOWN.quantity_number::text as "quantity_number",
       GEN_UNKNOWN.quantity_units::text as "quantity_units",
       GEN_UNKNOWN.has_access_policy::text as "has_access_policy",
       GEN_UNKNOWN.id::text as "id",
       GEN_UNKNOWN.Subject_id::text as "Subject_id",
       GEN_UNKNOWN.biospecimen_collection_id::text as "biospecimen_collection_id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype using (ftd_key)
    )

    select 
        * 
    from source
    