
    
    

with all_values as (

    select
        ethnicity as value_field,
        count(*) as n_records

    from "dbt"."main_PGRNseq_data"."PGRNseq_ftd_demographics"
    group by ethnicity

)

select *
from all_values
where value_field not in (
    'hispanic_or_latino','not_hispanic_or_latino','unknown','asked_but_unknown'
)


