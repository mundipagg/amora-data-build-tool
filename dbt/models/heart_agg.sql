{{
    config(
        materialized='table',
        partition_by={
            "field": "creationDate",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by='sourceName',
        tags=['daily']
    )
}}

SELECT
    avg(value) as avg_heart_rate,
    DAY(creationDate)
FROM
    {{ ref('heart_rate') }}
GROUP BY
    value
