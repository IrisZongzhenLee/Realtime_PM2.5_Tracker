{{ config(materialized='table') }}

select
    l.sensors_id,
    l.locations_id,
    m.location_name,
    m.city,
    l.datetime_local,
    l.pm25_value,
    m.parameter_units,
    l.latitude,
    l.longitude,
    case
        when l.pm25_value <= 15 then 'within_guideline'
        when l.pm25_value <= 35 then 'elevated'
        else 'high'
    end as pm25_category,
    case
        when l.pm25_value > 15 then true
        else false
    end as is_above_who_limit
from {{ ref('stg_airquality_us_sensor_metadata') }} as m
inner join {{ ref('stg_airquality_latest_pm25') }} as l
    on l.sensors_id = m.sensors_id
where l.pm25_value is not null