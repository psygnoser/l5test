{{
    config(
          schema='telemetry',
          materialized='table',
          unique_key=['vehicleId', 'dateTime']
    )
}}

with fendt as (

    select
        cast(vehicleId as STRING) as vehicleId,
        wialonVehicleId,
        timestamp,
        dateTime,
        gpsLongitude,
        gpsLatitude,
        transOilTempC,
        totalVehicleHours,
        oilPressureLpa,
        hydrOilLevel,
        hrsEngineHours,
        fuelLevelPct,
        batteryPotentialSwitchedV,
        engineSpeedRpm,
        coolantTemperatureC,
        catalystTankLevelPct,
        engineTripFuelL,
        totalDEFConsumptionL,
        wheelBasedVehicleSpeedKph,
        loadAtCurrSpeedPct,
        fuelRateLph,
        rearDraftKn,
        outdoorTempC,
        hitchPositionFrontPct,
        hitchPositionRearPct,
        PclRMeasuredPositionPct,
        PclRDraftKn,
        totalFuelUsedL,
        wheelSlipPct,
        workOn,
        diffLockState,
        airFilter,
        gearOilFilter,
        'fendt' as vendor
    from
        {{ ref('stg_fendt') }}
),

claas as (

    select
        vehicleId,
        wialonVehicleId,
        UNIX_SECONDS(dateTime) as timestamp,
        dateTime,
        gpsLongitude,
        gpsLatitude,
        null as transOilTempC,
        workingHours as totalVehicleHours,
        null as oilPressureLpa,
        null as hydrOilLevel,
        null as hrsEngineHours,
        null as fuelLevelPct,
        null as batteryPotentialSwitchedV,
        engineRpm as engineSpeedRpm,
        tempCoolantC as coolantTemperatureC,
        null as catalystTankLevelPct,
        null as engineTripFuelL,
        null as totalDEFConsumptionL,
        speedGearboxKph as wheelBasedVehicleSpeedKph,
        null as loadAtCurrSpeedPct,
        null as fuelRateLph,
        null as rearDraftKn,
        tempAmbientC as outdoorTempC,
        null as hitchPositionFrontPct,
        null as hitchPositionRearPct,
        null as PclRMeasuredPositionPct,
        null as PclRDraftKn,
        fuelConsumptionLph as totalFuelUsedL,
        null as wheelSlipPct,
        null as workOn,
        null as diffLockState,
        null as airFilter,
        null as gearOilFilter,
        'claas' as vendor
    from
        {{ ref('stg_claas') }}
),

combined as (

    select
        *
    from
        fendt

    union all

    select
        *
    from
        claas
),

decorated as (

    select
        combined.*,

        wialon.driverId,
        wialon.gpsLongitude as wialonGpsLongitude,
        wialon.gpsLatitude as wialonGpsLatitude,
        wialon.vehicleSpeed,
        wialon.vehicleAltitude,
        wialon.vehicleCourse
    from
        combined
    left join
        {{ ref('stg_wialon') }} as wialon
            on wialon.vehicleId = combined.wialonVehicleId
            and wialon.dateTime = combined.dateTime
)

select
    vehicleId,
    timestamp,
    dateTime,
    gpsLongitude,
    gpsLatitude,
    wialonGpsLongitude,
    wialonGpsLatitude,
    transOilTempC,
    totalVehicleHours,
    oilPressureLpa,
    hydrOilLevel,
    hrsEngineHours,
    fuelLevelPct,
    batteryPotentialSwitchedV,
    engineSpeedRpm,
    coolantTemperatureC,
    catalystTankLevelPct,
    engineTripFuelL,
    totalDEFConsumptionL,
    wheelBasedVehicleSpeedKph,
    loadAtCurrSpeedPct,
    fuelRateLph,
    rearDraftKn,
    outdoorTempC,
    hitchPositionFrontPct,
    hitchPositionRearPct,
    PclRMeasuredPositionPct,
    PclRDraftKn,
    totalFuelUsedL,
    wheelSlipPct,
    workOn,
    diffLockState,
    airFilter,
    gearOilFilter,
    driverId,
    vehicleSpeed,
    vehicleAltitude,
    vehicleCourse,
    vendor
from
    decorated
