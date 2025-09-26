
    
    

with all_values as (

    select
        race as value_field,
        count(*) as n_records

    from "dbt"."main_cmg_bh_data"."cmg_bh_ftd_demographics_race"
    group by race

)

select *
from all_values
where value_field not in (
    'american_indian_or_alaskan_native','asian','black_or_african_american','native_hawaiian_or_pacific_islander','white','other_race','unknown','asked_but_unknown'
)


