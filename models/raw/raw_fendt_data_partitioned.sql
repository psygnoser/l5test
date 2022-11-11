{{
    config(
          schema='raw',
          materialized='view'
    )
}}

{{ daily_dumps('fendt_data_partitioned_*', 3) }}
