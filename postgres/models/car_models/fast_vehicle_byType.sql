{{ config(materialzied='view')}}

with fast_v as (select * from {{ref('fast_car_dbt_model')}})

SELECT 
Type as "Vehicle type",
count(Type) as "vehicle count"
from fast_v 
GROUP BY Type ORDER BY "vehicle count" ASC