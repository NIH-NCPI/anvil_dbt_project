with
unpivot_df as (select
            distinct 
            file_id as "file_id",
            'crc32c' as "display",
            cast(crc32c as varchar) as "value_display",
            CONCAT(ftd_index,'_fi') as "ftd_index"
        from "dbt"."main_main"."cser_stg_file_inventory"
        where crc32c IS NOT NULL
        union all
    select
            distinct 
            file_id as "file_id",
            'md5_hash' as "display",
            cast(md5_hash as varchar) as "value_display",
            CONCAT(ftd_index,'_fi') as "ftd_index"
        from "dbt"."main_main"."cser_stg_file_inventory"
        where md5_hash IS NOT NULL
        
    
        union all
   
    
        select
            distinct 
            sequencing_id as "file_id",
            'alignment_method' as "display",
            cast(alignment_method as varchar) as "value_display",
            CONCAT(ftd_index,'_seq') as "ftd_index"
        from "dbt"."main_main"."cser_stg_sequencing"
        where alignment_method IS NOT NULL
        union all
    
        select
            distinct 
            sequencing_id as "file_id",
            'analyte_type' as "display",
            cast(analyte_type as varchar) as "value_display",
            CONCAT(ftd_index,'_seq') as "ftd_index"
        from "dbt"."main_main"."cser_stg_sequencing"
        where analyte_type IS NOT NULL
        union all
    
        select
            distinct 
            sequencing_id as "file_id",
            'functional_equivalence_standard' as "display",
            cast(functional_equivalence_standard as varchar) as "value_display",
            CONCAT(ftd_index,'_seq') as "ftd_index"
        from "dbt"."main_main"."cser_stg_sequencing"
        where functional_equivalence_standard IS NOT NULL
        union all
    
        select
            distinct 
            sequencing_id as "file_id",
            'library_prep_kit_method' as "display",
            cast(library_prep_kit_method as varchar) as "value_display",
            CONCAT(ftd_index,'_seq') as "ftd_index"
        from "dbt"."main_main"."cser_stg_sequencing"
        where library_prep_kit_method IS NOT NULL
        union all
    
        select
            distinct 
            sequencing_id as "file_id",
            'reference_genome_build' as "display",
            cast(reference_genome_build as varchar) as "value_display",
            CONCAT(ftd_index,'_seq') as "ftd_index"
        from "dbt"."main_main"."cser_stg_sequencing"
        where reference_genome_build IS NOT NULL
        
    
)
select distinct
NULL::text as "code",
display::text as "display",
NULL::text as "value_code",
value_display::text as "value_display",
    'fd' || '_' || md5('cser' || '|' || cast(coalesce(file_id, '') as text))::text as "id"
from unpivot_df