{% macro daily_dumps(table_pattern, n) %}

    select
        *
    from
        `raw.{{ table_pattern }}`
    {% if not flags.FULL_REFRESH %}
    where
        _TABLE_SUFFIX between cast(date_sub(current_date(), interval {{ n }} day) as string) and cast(current_date() as string) -- select a union of last n daily dumps
    {% endif %}

{% endmacro %}