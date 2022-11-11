{% macro extract_wialon_dump(date_suffix) %}

    {% set file = 'wialon_dump' %}
    {% set format = 'CSV' %}
    {% set bucket_path = 'daily_telematics/' %}
    {% set source_file = bucket_path ~ file ~ '_' ~ date_suffix ~ '.csv' %}
    {% set target_table = '`raw.' ~ file ~ '_' ~ date_suffix ~ '`' %}

    {%- set query %}
        -- using automatic schema detection; using explicit schema would be preferable in production
        LOAD DATA OVERWRITE {{ target_table }}
            FROM FILES(
            skip_leading_rows=1,
            format='{{ format }}',
            uris = ['gs://{{ source_file }}']
        );

    {% endset -%}

    {% do log(query, info=true) %}

    {% do run_query(query) %}

{% endmacro %}
