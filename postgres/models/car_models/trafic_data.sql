{{ config(materialized='view') }}

with traffic_datas as (

    select * from Trajectory

)

select *
from traffic_datas