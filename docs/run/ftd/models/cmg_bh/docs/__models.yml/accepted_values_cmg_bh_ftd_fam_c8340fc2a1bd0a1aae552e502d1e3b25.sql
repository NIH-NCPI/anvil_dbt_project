
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        consanguinity as value_field,
        count(*) as n_records

    from "dbt"."main_cmg_bh_data"."cmg_bh_ftd_family"
    group by consanguinity

)

select *
from all_values
where value_field not in (
    'not_suspected','suspected','known_present','unknown'
)



  
  
      
    ) dbt_internal_test