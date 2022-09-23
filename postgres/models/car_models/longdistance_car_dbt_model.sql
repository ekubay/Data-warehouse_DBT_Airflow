{{ config(materialized='view') }}

with source_data as (

    select * from cars where avg_speed>40

)

select *
from source_data