{{
    config(
          schema='raw',
          materialized='view'
    )
}}

{{ daily_dumps('fendt_gps_*', 3) }}
