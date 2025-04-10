{{ config(
    materialized = 'table',
    alias = 'dim_company',
    tags = ['dm', 'jobs', 'company']
) }}

WITH DUMMY AS (
    SELECT
        HASH(-1)                AS DIM_COMPANY_KEY,
        'UNKNOWN'               AS COMPANY_NAME,
        'UNKNOWN'               AS COMPANY_LOGO,
        CURRENT_TIMESTAMP()     AS META_INSERT_DATE
),

SOURCE AS (
    SELECT DISTINCT
        HASH(COMPANY)           AS DIM_COMPANY_KEY,
        COMPANY                 AS COMPANY_NAME,
        COMPANY_LOGO            AS COMPANY_LOGO,
        CURRENT_TIMESTAMP()     AS META_INSERT_DATE
    FROM {{ ref('jobs_current') }}
    WHERE COMPANY IS NOT NULL
)

SELECT * FROM DUMMY
UNION ALL
SELECT * FROM SOURCE
