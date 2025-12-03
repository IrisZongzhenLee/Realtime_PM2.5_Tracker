-- models/staging/staging_airquality_;atest_pm25.sql

SELECT
    sensors_id,
    locations_id,
    to_timestamp_ntz(datetime_utc)   AS datetime_utc,
    to_timestamp_ntz(datetime_local) AS datetime_local,
    cast(value_ugm_3 as float)        AS pm25_value,
    cast(latitude  as float)         AS latitude,
    cast(longitude as float)         AS longitude
FROM {{ source('AIRQUALITY_SDK', 'AIRQUALITY_LATEST_PM_25') }}
WHERE sensors_id IS NOT NULL
  AND pm25_value IS NOT NULL
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL