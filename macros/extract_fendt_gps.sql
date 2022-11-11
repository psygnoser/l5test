{% macro extract_fendt_gps(date_suffix) %}

    {% set file = 'fendt_gps' %}
    {% set format = 'JSON' %}
    {% set bucket_path = 'daily_telematics/' %}
    {% set source_file = bucket_path ~ file ~ '_' ~ date_suffix ~ '.json' %}
    {% set target_table = '`raw.' ~ file ~ '_' ~ date_suffix ~ '`' %}

    {%- set query %}
        -- using automatic schema detection; using explicit schema would be preferable in production
        LOAD DATA OVERWRITE {{ target_table }}
            FROM FILES(
            format='{{ format }}',
            uris = ['gs://{{ source_file }}']
        );

    {% endset -%}

    {% do log(query, info=true) %}

    {% do run_query(query) %}

{% endmacro %}
