{{ config(materialized='table', schema='cmg_uwash_data') }}

{%- set consent_groups = ['gru', 'ds', 'hmb', 'irb', 'pub'] -%}
  {%- for grp in consent_groups %}
    select distinct
      {{ generate_global_id(prefix='ap', descriptor=['consent_group'], study_id='phs000693') }}::text as accesspolicy_id,
      '{{ grp }}' as access_policy_code
    from (select distinct consent_group from {{ ref('cmg_uwash_stg_anvil_dataset') }}) as ad
    where ad.consent_group ILIKE '%{{ grp }}%'
    {%- if not loop.last %} union all {% endif %}
  {%- endfor %}

