with
unpivot_df as (select
            distinct 
            name as "filename",
            'crc32c' as "code",
            cast(crc32c as varchar) as "value_code",
            CONCAT(ftd_index,'_fi') as "ftd_index"
        from "dbt"."main_main"."cmg_yale_stg_file_inventory"
        where crc32c IS NOT NULL
        union all
    select
            distinct 
            name as "filename",
            'md5_hash' as "code",
            cast(md5_hash as varchar) as "value_code",
            CONCAT(ftd_index,'_fi') as "ftd_index"
        from "dbt"."main_main"."cmg_yale_stg_file_inventory"
        where md5_hash IS NOT NULL
        
    
        union all
   
    
        select
            distinct 
            sequencing_id as "filename",
            'alignment_method' as "code",
            cast(alignment_method as varchar) as "value_code",
            CONCAT(ftd_index,'_seq') as "ftd_index"
        from "dbt"."main_main"."cmg_yale_stg_sequencing"
        where alignment_method IS NOT NULL
        union all
    
        select
            distinct 
            sequencing_id as "filename",
            'analyte_type' as "code",
            cast(analyte_type as varchar) as "value_code",
            CONCAT(ftd_index,'_seq') as "ftd_index"
        from "dbt"."main_main"."cmg_yale_stg_sequencing"
        where analyte_type IS NOT NULL
        union all
    
        select
            distinct 
            sequencing_id as "filename",
            'functional_equivalence_standard' as "code",
            cast(functional_equivalence_standard as varchar) as "value_code",
            CONCAT(ftd_index,'_seq') as "ftd_index"
        from "dbt"."main_main"."cmg_yale_stg_sequencing"
        where functional_equivalence_standard IS NOT NULL
        union all
    
        select
            distinct 
            sequencing_id as "filename",
            'library_prep_kit_method' as "code",
            cast(library_prep_kit_method as varchar) as "value_code",
            CONCAT(ftd_index,'_seq') as "ftd_index"
        from "dbt"."main_main"."cmg_yale_stg_sequencing"
        where library_prep_kit_method IS NOT NULL
        union all
    
        select
            distinct 
            sequencing_id as "filename",
            'reference_genome_build' as "code",
            cast(reference_genome_build as varchar) as "value_code",
            CONCAT(ftd_index,'_seq') as "ftd_index"
        from "dbt"."main_main"."cmg_yale_stg_sequencing"
        where reference_genome_build IS NOT NULL
        union all
    
        select
            distinct 
            sequencing_id as "filename",
            'sequencing_assay' as "code",
            cast(sequencing_assay as varchar) as "value_code",
            CONCAT(ftd_index,'_seq') as "ftd_index"
        from "dbt"."main_main"."cmg_yale_stg_sequencing"
        where sequencing_assay IS NOT NULL
        
    
)

select 
  distinct
  NULL::text as "code",
  code::text as "display",
  NULL::text as "value_code",
  value_code::text as "value_display",
  'fd' || '_' || md5('cmg_yale' || '|' || cast(coalesce(filename, '') as text))::text as "id"
from unpivot_df