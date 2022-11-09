{{ config(materialized='view') }}

with source_data as (
    select * from Trajectory where AVg_Speed >37 order by Type
)
select *
from source_data