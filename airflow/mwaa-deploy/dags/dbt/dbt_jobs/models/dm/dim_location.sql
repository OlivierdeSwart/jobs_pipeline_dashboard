{{ config(
    materialized = 'table',
    alias = 'dim_location',
    tags = ['dm', 'jobs', 'location']
) }}

WITH DUMMY AS (
    SELECT
        HASH(-1)                AS DIM_LOCATION_KEY,
        'UNKNOWN'               AS LOCATION_NAME,
        CURRENT_TIMESTAMP()     AS META_INSERT_DATE
),

SOURCE AS (
    SELECT DISTINCT
        HASH(CANDIDATE_LOCATION) AS DIM_LOCATION_KEY,
        CANDIDATE_LOCATION       AS LOCATION_NAME,
        CURRENT_TIMESTAMP()      AS META_INSERT_DATE
    FROM {{ ref('jobs_versioning') }}
    WHERE CANDIDATE_LOCATION IS NOT NULL
)

SELECT * FROM DUMMY
UNION ALL
SELECT * FROM SOURCE
