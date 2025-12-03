-- models/staging/stg_page_views.sql

{{ config(materialized='view') }}

SELECT
    sensors_id,
    parameter_name,
    parameter_units,
    locations_id,
    location_name,
    city,
    cast(latitude  as float) AS latitude,
    cast(longitude as float) AS longitude
FROM {{ source('AIRQUALITY_SDK', 'AIRQUALITY_SENSOR_METADATA') }}
WHERE sensors_id IS NOT NULL
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL