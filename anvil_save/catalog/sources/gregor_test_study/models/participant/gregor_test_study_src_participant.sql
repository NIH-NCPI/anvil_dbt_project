{{ config(materialized='table') }}

select * from anvil.participant
