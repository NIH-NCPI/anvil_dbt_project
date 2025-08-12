
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        family_type as value_field,
        count(*) as n_records

    from "dbt"."main_cmg_bh_data"."cmg_bh_ftd_family"
    group by family_type

)

select *
from all_values
where value_field not in (
    'control_only','duo','proband_only','trio','trio_plus','other'
)



  
  
      
    ) dbt_internal_test