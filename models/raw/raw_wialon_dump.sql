{{
    config(
          schema='raw',
          materialized='view'
    )
}}

{{ daily_dumps('wialon_dump_*', 3) }}
