{{
    config(
          schema='staging',
          materialized='incremental',
          incremental_strategy='merge',
          unique_key=['vehicleId', 'dateTime']
    )
}}

with wialon as (

    select
        unit_id as vehicleId,
        dateTime,
        driverId,
        gpsLongitude,
        gpsLatitude,
        speed as vehicleSpeed,
        altitude as vehicleAltitude,
        course as vehicleCourse
    from
        {{ ref('raw_wialon_dump') }}
    {% if is_incremental() %}
    where
        dateTime > (select max(dateTime) from {{ this }})
    {% endif %}
)

select
    vehicleId,
    dateTime,
    driverId,
    gpsLongitude,
    gpsLatitude,
    vehicleSpeed,
    vehicleAltitude,
    vehicleCourse
from
    wialon
