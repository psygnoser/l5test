{{
    config(
          schema='staging',
          materialized='incremental',
          incremental_strategy='merge',
          unique_key=['vehicleId', 'dateTime']
    )
}}

with fendt_part_raw as (

    select
        machineId,
        count,
        datas
    from
        {{ ref('raw_fendt_data_partitioned') }}
),

fendt_part as (

    select
        fendt_part_raw.machineId as vehicleId,

        data.type,
        data.signalGroup,
        data.unit,

        dataValues.timestamp,
        dataValues.value,
    from
        fendt_part_raw,
            unnest (datas) as data,
            unnest (data.values) as dataValues
    where
        dataValues.value is not null
    {% if is_incremental() %}
        and dataValues.timestamp > (select max(timestamp) from {{ this }})
    {% endif %}
),

fendt_gps_raw as (

    select
        machineId as vehicleId,
        route
    from
        {{ ref('raw_fendt_gps') }}
),

fendt_gps as (

    select
        fendt_gps_raw.vehicleId,

        dataValues.t as timestamp,
        dataValues.lng as gpsLongitude,
        dataValues.lat as gpsLatitude
    from
        fendt_gps_raw,
            unnest (route) as dataValues
    {% if is_incremental() %}
        where
            dataValues.t > (select max(timestamp) from {{ this }})
    {% endif %}
),

fendt_combined as (

    select
        fendt_part.vehicleId || fendt_part.timestamp || fendt_gps.gpsLongitude || fendt_gps.gpsLatitude as uid,
        fendt_part.vehicleId,
        fendt_part.timestamp,
        fendt_part.type,
        fendt_part.value,

        timestamp_seconds(fendt_part.timestamp) as dateTime,

        fendt_gps.gpsLongitude,
        fendt_gps.gpsLatitude
    from
        fendt_part
    left join
        fendt_gps
            on fendt_part.vehicleId = fendt_gps.vehicleId
            and fendt_part.timestamp = fendt_gps.timestamp
            and fendt_gps.gpsLongitude is not null
    order by
        fendt_part.vehicleId asc,
        fendt_part.timestamp asc
),

fendt_combined_distinct as (

    select
        distinct uid, vehicleId, timestamp, dateTime, gpsLongitude, gpsLatitude
    from
        fendt_combined
),

flattened as (
    select
        fendt_combined_distinct.vehicleId,
        fendt_combined_distinct.timestamp,
        fendt_combined_distinct.dateTime,
        fendt_combined_distinct.gpsLongitude,
        fendt_combined_distinct.gpsLatitude,

        fendt_combined_TransOilTemp.value as transOilTempC,
        fendt_combined_TOTAL_VEHICLE_HOURS.value as totalVehicleHours,
        fendt_combined_OilPressure.value as oilPressureLpa,
        fendt_combined_HYDR_OIL_LEVEL.value as hydrOilLevel,
        fendt_combined_HRSengineHours.value as hrsEngineHours,
        fendt_combined_FuelLevel.value as fuelLevelPct,
        fendt_combined_BatteryPotentialSwitched.value as batteryPotentialSwitchedV,
        fendt_combined_EngineSpeed.value as engineSpeedRpm,
        fendt_combined_CoolantTemperature.value as coolantTemperatureC,
        fendt_combined_CatalystTankLevel.value as catalystTankLevelPct,
        fendt_combined_ENGINE_TRIP_FUEL.value as engineTripFuelL,
        fendt_combined_TotalDEFConsumption.value as totalDEFConsumptionL,
        fendt_combined_WheelBasedVehicleSpeed.value as wheelBasedVehicleSpeedKph,
        fendt_combined_LoadAtCurrSpeed.value as loadAtCurrSpeedPct,
        fendt_combined_FuelRate.value as fuelRateLph,
        fendt_combined_REAR_DRAFT.value as rearDraftKn,
        fendt_combined_OutdoorTemp.value as outdoorTempC,
        fendt_combined_HITCH_POSITION_FRONT.value as hitchPositionFrontPct,
        fendt_combined_HITCH_POSITION_REAR.value as hitchPositionRearPct,
        fendt_combined_PLC_R_Measured_position.value as PclRMeasuredPositionPct,
        fendt_combined_PLC_R_Draft.value as PclRDraftKn,
        fendt_combined_TotalFuelUsed.value as totalFuelUsedL,
        fendt_combined_WHEEL_SLIP.value as wheelSlipPct,
        fendt_combined_WORK_ON.value as workOn,
        fendt_combined_DiffLockState.value as diffLockState,
        fendt_combined_AirFilter.value as airFilter,
        fendt_combined_GearOilFilter.value as gearOilFilter
    from
        fendt_combined_distinct
    left join
        fendt_combined as fendt_combined_TransOilTemp
            on fendt_combined_distinct.uid = fendt_combined_TransOilTemp.uid
            and fendt_combined_TransOilTemp.type = 'TransOilTemp'
    left join
        fendt_combined as fendt_combined_TOTAL_VEHICLE_HOURS
            on fendt_combined_distinct.uid = fendt_combined_TOTAL_VEHICLE_HOURS.uid
            and fendt_combined_TOTAL_VEHICLE_HOURS.type = 'TOTAL_VEHICLE_HOURS'
    left join
        fendt_combined as fendt_combined_OilPressure
            on fendt_combined_distinct.uid = fendt_combined_OilPressure.uid
            and fendt_combined_OilPressure.type = 'OilPressure'
    left join
        fendt_combined as fendt_combined_HYDR_OIL_LEVEL
            on fendt_combined_distinct.uid = fendt_combined_HYDR_OIL_LEVEL.uid
            and fendt_combined_HYDR_OIL_LEVEL.type = 'HYDR_OIL_LEVEL'
    left join
        fendt_combined as fendt_combined_HRSengineHours
            on fendt_combined_distinct.uid = fendt_combined_HRSengineHours.uid
            and fendt_combined_HRSengineHours.type = 'HRSengineHours'
    left join
        fendt_combined as fendt_combined_FuelLevel
            on fendt_combined_distinct.uid = fendt_combined_FuelLevel.uid
            and fendt_combined_FuelLevel.type = 'FuelLevel'
    left join
        fendt_combined as fendt_combined_BatteryPotentialSwitched
            on fendt_combined_distinct.uid = fendt_combined_BatteryPotentialSwitched.uid
            and fendt_combined_BatteryPotentialSwitched.type = 'BatteryPotentialSwitched'
    left join
        fendt_combined as fendt_combined_EngineSpeed
            on fendt_combined_distinct.uid = fendt_combined_EngineSpeed.uid
            and fendt_combined_EngineSpeed.type = 'EngineSpeed'
    left join
        fendt_combined as fendt_combined_CoolantTemperature
            on fendt_combined_distinct.uid = fendt_combined_CoolantTemperature.uid
            and fendt_combined_CoolantTemperature.type = 'CoolantTemperature'
    left join
        fendt_combined as fendt_combined_CatalystTankLevel
            on fendt_combined_distinct.uid = fendt_combined_CatalystTankLevel.uid
            and fendt_combined_CatalystTankLevel.type = 'CatalystTankLevel'
    left join
        fendt_combined as fendt_combined_ENGINE_TRIP_FUEL
            on fendt_combined_distinct.uid = fendt_combined_ENGINE_TRIP_FUEL.uid
            and fendt_combined_ENGINE_TRIP_FUEL.type = 'ENGINE_TRIP_FUEL'
    left join
        fendt_combined as fendt_combined_TotalDEFConsumption
            on fendt_combined_distinct.uid = fendt_combined_TotalDEFConsumption.uid
            and fendt_combined_TotalDEFConsumption.type = 'TotalDEFConsumption'
    left join
        fendt_combined as fendt_combined_WheelBasedVehicleSpeed
            on fendt_combined_distinct.uid = fendt_combined_WheelBasedVehicleSpeed.uid
            and fendt_combined_WheelBasedVehicleSpeed.type = 'WheelBasedVehicleSpeed'
    left join
        fendt_combined as fendt_combined_LoadAtCurrSpeed
            on fendt_combined_distinct.uid = fendt_combined_LoadAtCurrSpeed.uid
            and fendt_combined_LoadAtCurrSpeed.type = 'LoadAtCurrSpeed'
    left join
        fendt_combined as fendt_combined_FuelRate
            on fendt_combined_distinct.uid = fendt_combined_FuelRate.uid
            and fendt_combined_FuelRate.type = 'FuelRate'
    left join
        fendt_combined as fendt_combined_REAR_DRAFT
            on fendt_combined_distinct.uid = fendt_combined_REAR_DRAFT.uid
            and fendt_combined_REAR_DRAFT.type = 'REAR_DRAFT'
    left join
        fendt_combined as fendt_combined_OutdoorTemp
            on fendt_combined_distinct.uid = fendt_combined_OutdoorTemp.uid
            and fendt_combined_OutdoorTemp.type = 'OutdoorTemp'
    left join
        fendt_combined as fendt_combined_HITCH_POSITION_FRONT
            on fendt_combined_distinct.uid = fendt_combined_HITCH_POSITION_FRONT.uid
            and fendt_combined_HITCH_POSITION_FRONT.type = 'HITCH_POSITION_FRONT'
    left join
        fendt_combined as fendt_combined_HITCH_POSITION_REAR
            on fendt_combined_distinct.uid = fendt_combined_HITCH_POSITION_REAR.uid
            and fendt_combined_HITCH_POSITION_REAR.type = 'HITCH_POSITION_REAR'
    left join
        fendt_combined as fendt_combined_PLC_R_Measured_position
            on fendt_combined_distinct.uid = fendt_combined_PLC_R_Measured_position.uid
            and fendt_combined_PLC_R_Measured_position.type = 'PLC_R_Measured_position'
    left join
        fendt_combined as fendt_combined_PLC_R_Draft
            on fendt_combined_distinct.uid = fendt_combined_PLC_R_Draft.uid
            and fendt_combined_PLC_R_Draft.type = 'PLC_R_Draft'
    left join
        fendt_combined as fendt_combined_TotalFuelUsed
            on fendt_combined_distinct.uid = fendt_combined_TotalFuelUsed.uid
            and fendt_combined_TotalFuelUsed.type = 'TotalFuelUsed'
    left join
        fendt_combined as fendt_combined_WHEEL_SLIP
            on fendt_combined_distinct.uid = fendt_combined_WHEEL_SLIP.uid
            and fendt_combined_WHEEL_SLIP.type = 'WHEEL_SLIP'
    left join
        fendt_combined as fendt_combined_AirFilter
            on fendt_combined_distinct.uid = fendt_combined_AirFilter.uid
            and fendt_combined_AirFilter.type = 'AirFilter'
    left join
        fendt_combined as fendt_combined_GearOilFilter
            on fendt_combined_distinct.uid = fendt_combined_GearOilFilter.uid
            and fendt_combined_GearOilFilter.type = 'GearOilFilter'
    left join
        fendt_combined as fendt_combined_WORK_ON
            on fendt_combined_distinct.uid = fendt_combined_WORK_ON.uid
            and fendt_combined_WORK_ON.type = 'WORK_ON'
    left join
        fendt_combined as fendt_combined_DiffLockState
            on fendt_combined_distinct.uid = fendt_combined_DiffLockState.uid
            and fendt_combined_DiffLockState.type = 'DiffLockState'
),

mapped as (

    select
        flattened.*,

        wialon_mapping.wialonVehicleId
    from
        flattened
    left join
        {{ ref('wialon_mapping') }} as wialon_mapping
            on wialon_mapping.telemetryId = cast(flattened.vehicleId as STRING)
)

select
    vehicleId,
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
    gearOilFilter
from
    mapped
