{{ config(materialized='view') }}

with source_data as (
    select type, traveled_d, avg_speed, time from cars where avg_speed > 40  
)
select *
from source_data