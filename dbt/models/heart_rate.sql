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

WITH source AS(
    SELECT
        *
    FROM
        {{ ref('health') }}
    WHERE
        type = 'HeartRate'
)

SELECT
    sourceName,
    sourceVersion,
    unit,
    creationDate,
    startDate,
    endDate,
    value,
    device
FROM
    source
