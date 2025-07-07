#!/usr/bin/env python

'''
Generates using csvs, for example data/static/enumerations/{study_id}_enums.csv

Example csv
src_table,src_field,expected_src_value,equivalent_model_value,tgt_field,in_use
"clinical","assertion",'1','Observed',"condition_interpretation",T
"clinical","assertion",'0','Not Observed',"condition_interpretation",T
"clinical","assertion",is null,null,"condition_interpretation",T
"clinical","assertion","else",null,"condition_interpretation",T

Results in the terminal would be:
    case
    when clinical.assertion = '1'
        then 'Observed'
    when  clinical.assertion = '0'
        then 'Not Observed'
    when  clinical.assertion is null
        then null
    else null
end as "condition_interpretation",

'''
import argparse
from dbt_pipeline_utils.scripts.transformation_helpers.generate_case_statements import main

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ETL script with arguments.")
    parser.add_argument("-s", "--study_id", required=True, help="The study identifier.")

    args = parser.parse_args()

    main(study_id=args.study_id)
    
