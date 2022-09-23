{{ config(materialized='view') }}

with Longest_distance as (
    select * from {{ref('fast_car_dbt_model')}} 
        order by traveled_d ASC 
)
select * from Longest_distance