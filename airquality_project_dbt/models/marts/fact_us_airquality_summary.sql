{{ config(materialized='table') }}

select
    city,
    count(*)                                as num_sensors,
    avg(pm25_value)                          as avg_pm25_value,
    max(pm25_value)                          as max_pm25_value,
    min(pm25_value)                          as min_pm25_value,
    sum(case when is_above_who_limit then 1 else 0 end) as sensors_above_who,
    sum(case when not is_above_who_limit then 1 else 0 end) as sensors_within_who
from {{ ref('us_airquality_latest') }}
where city is not null
group by city