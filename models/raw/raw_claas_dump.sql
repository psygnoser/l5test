{{
    config(
          schema='raw',
          materialized='view'
    )
}}

{{ daily_dumps('claas_dump_*', 3) }}
