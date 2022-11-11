{{
    config(
          schema='telemetry',
          materialized='table',
          unique_key=['vehicleId', 'date']
    )
}}

with combined_lag as (

    select
        lag(timestamp) over (partition by vehicleId order by timestamp asc) as prevTimestamp,
        lag(gpsLongitude) over (partition by vehicleId order by timestamp asc) as prevLongitude,
        lag(gpsLatitude) over (partition by vehicleId order by timestamp asc) as prevLatitude,

        vehicleId,
        dateTime,
        timestamp,
        gpsLongitude,
        gpsLatitude,
        vendor
    from
        {{ ref('combined') }}
),

combined_geopoint as (

    select
        (
            timestamp - prevTimestamp > 15 * 60 -- (Seconds. Assumes a longer than 15min break indicates a new run ie. power-off/on)
            or prevTimestamp is null
        ) as isNewRun,

        st_geogpoint(prevLongitude, prevLatitude) as prevGeoPoint,
        st_geogpoint(gpsLongitude, gpsLatitude) as geoPoint,

       *
    from
        combined_lag
),

combined_run as (

    select
        sum(case when isNewRun then 1 end) over (
            partition by vehicleId
            order by timestamp desc rows between unbounded preceding and 1 preceding
        ) as runId,

        *
    from
        combined_geopoint
),

combined_run_extra as (

    select
        case
            when not isNewRun
                then
                    st_distance(
                        prevGeoPoint,
                        geoPoint
                    )
                else 0
        end as eventDistance,

        first_value(geoPoint) over (
            partition by vehicleId, runId
            order by timestamp asc
        ) as runStartPoint,

        first_value(geoPoint) over (
            partition by vehicleId, runId
            order by timestamp desc
        ) as runEndPoint,

        first_value(dateTime) over (
            partition by vehicleId, runId
            order by timestamp asc
        ) as runStartDatetime,

        first_value(dateTime) over (
            partition by vehicleId, runId
            order by timestamp desc
        ) as runEndDatetime,

        *
    from
        combined_run
),

combined_run_distance as (

    select
        sum(eventDistance) over (partition by vehicleId, runId) as runDistanceM,

        *
    from
        combined_run_extra
),

transformed as (

    select
        cast(dateTime as date) as date,

        runDistanceM / 1e+3  as distanceTraveledKm,

        vehicleId,
        dateTime,
        runStartDatetime,
        runEndDatetime,
        runStartPoint,
        runEndPoint,
        vendor
    from
        combined_run_distance
    where
        isNewRun
)

select
    date,
    vehicleId,
    runStartDatetime,
    runEndDatetime,
    runStartPoint,
    runEndPoint,
    distanceTraveledKm,
    vendor
from
    transformed
