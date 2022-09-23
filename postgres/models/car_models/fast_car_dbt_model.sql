{{ config(materialized='view') }}

with source_data as (
    select * from cars where avg_speed>37 order by type
)
select *
from source_data