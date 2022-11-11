{{
    config(
          schema='staging',
          materialized='incremental',
          incremental_strategy='merge',
          unique_key=['vehicleId', 'dateTime']
    )
}}

with claas as (

    select
        SerialNumber as vehicleId,
        GearShift as vehicleGearShift,
        AllWheelDriveStatus as vehicleAllWheelDriveStatus,
        CreeperStatus as vehicleCreeperStatus,
        cast(ParkingBreakStatus as INT64) as parkingBreakStatus,
        cast(DifferentialLockStatus as INT64) as differentialLockStatus,
        cast(`DateTime` as TIMESTAMP) as dateTime,
        cast(GpsLongitude as FLOAT64) as gpsLongitude,
        cast(GpsLatitude as FLOAT64) as gpsLatitude,
        cast(TotalWorkingHours as FLOAT64) as workingHours,
        cast(Engine_rpm as FLOAT64) as engineRpm,
        cast(EngineLoad as FLOAT64) as engineLoad,
        cast(FuelConsumption_l_h as FLOAT64) as fuelConsumptionLph,
        cast(SpeedGearbox_km_h as FLOAT64) as speedGearboxKph,
        cast(SpeedRadar_km_h as FLOAT64) as speedRadarKph,
        cast(TempCoolant_C as FLOAT64) as tempCoolantC,
        cast(PtoFront_rpm as FLOAT64) as ptoFrontRpm,
        cast(PtoRear_rpm as FLOAT64) as proRearRpm,
        cast(TempAmbient_C as FLOAT64) as tempAmbientC
    from
        {{ ref('raw_claas_dump') }}
    {% if is_incremental() %}
    where
        `DateTime` > (select max(`DateTime`) from {{ this }})
    {% endif %}
),

mapped as (

    select
        claas.*,

        wialon_mapping.wialonVehicleId
    from
        claas
    left join
        {{ ref('wialon_mapping') }} as wialon_mapping
            on wialon_mapping.telemetryId = claas.vehicleId
)

select
    vehicleId,
    wialonVehicleId,
    dateTime,
    vehicleGearShift,
    vehicleAllWheelDriveStatus,
    vehicleCreeperStatus,
    parkingBreakStatus,
    differentialLockStatus,
    gpsLongitude,
    gpsLatitude,
    workingHours,
    engineRpm,
    engineLoad,
    fuelConsumptionLph,
    speedGearboxKph,
    speedRadarKph,
    tempCoolantC,
    ptoFrontRpm,
    proRearRpm,
    tempAmbientC
from
    mapped
