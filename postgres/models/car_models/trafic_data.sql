{{ config(materialized='view') }}

with traffic_datas as (

    select * from trajectory

)

select *
from traffic_datas