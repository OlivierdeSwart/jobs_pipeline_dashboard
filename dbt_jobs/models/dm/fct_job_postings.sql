{{ config(
    materialized = 'table',
    alias = 'fct_job_postings',
    tags = ['dm', 'jobs', 'fact']
) }}

WITH SOURCE AS (
    SELECT
        HASH(JOB_ID)                AS FCT_JOB_POSTING_KEY,
        HASH(COMPANY)               AS DIM_COMPANY_KEY,
        HASH(CANDIDATE_LOCATION)    AS DIM_LOCATION_KEY,
        JOB_ID,
        TITLE,
        CATEGORY,
        URL,
        JOB_TYPE,
        PUBLISHED_AT,
        SALARY,
        DESCRIPTION,
        META_INSERT_DATE
    FROM {{ ref('jobs_current') }}
    WHERE META_IS_DELETED = 0
)

SELECT * FROM SOURCE
