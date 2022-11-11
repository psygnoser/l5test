{% macro extract_claas_dump(date_suffix) %}

    {% set file = 'claas_dump' %}
    {% set format = 'CSV' %}
    {% set bucket_path = 'daily_telematics/' %}
    {% set source_file = bucket_path ~ file ~ '_' ~ date_suffix ~ '.csv' %}
    {% set target_table = '`raw.' ~ file ~ '_' ~ date_suffix ~ '`' %}

    {%- set query %}

        LOAD DATA OVERWRITE {{ target_table }}(
            DateTime TIMESTAMP, SerialNumber STRING, GpsLongitude FLOAT64, GpsLatitude FLOAT64, TotalWorkingHours FLOAT64, Engine_rpm FLOAT64, EngineLoad FLOAT64, FuelConsumption_l_h FLOAT64, SpeedGearbox_km_h FLOAT64, SpeedRadar_km_h FLOAT64, TempCoolant_C FLOAT64, PtoFront_rpm FLOAT64, PtoRear_rpm FLOAT64, GearShift FLOAT64, TempAmbient_C FLOAT64, ParkingBreakStatus FLOAT64, DifferentialLockStatus FLOAT64, AllWheelDriveStatus STRING, CreeperStatus STRING
        )
            FROM FILES(
            skip_leading_rows=1,
            format='{{ format }}',
            uris = ['gs://{{ source_file }}']
        );

    {% endset -%}

    {% do log(query, info=true) %}

    {% do run_query(query) %}

{% endmacro %}
