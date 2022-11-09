{{ config(materialized='view') }}

with Longest_distance as (
    select * from {{ref('fast_car_dbt_model')}} 
        order by Traveled_Dis ASC 
)
select * from Longest_distance