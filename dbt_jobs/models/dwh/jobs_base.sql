{{ config(
    materialized = 'incremental',
    unique_key = ['job_id', 'meta_insert_date'],
    incremental_strategy = 'append',
    on_schema_change = 'append_new_columns',
    tags = ['dwh', 'jobs'],
    alias = 'jobs_base'
) }}

WITH SOURCE_DATA AS (

    SELECT
        RAW_DATA:"id"::INT              AS JOB_ID,
        RAW_DATA:"title"::STRING        AS TITLE,
        RAW_DATA:"company_name"::STRING AS COMPANY,
        RAW_DATA:"category"::STRING     AS CATEGORY,
        RAW_DATA:"url"::STRING          AS URL,
        RAW_DATA:"job_type"::STRING     AS JOB_TYPE,
        RAW_DATA:"publication_date"::TIMESTAMP AS PUBLISHED_AT,

        -- SCD2 META
        CURRENT_TIMESTAMP()             AS META_INSERT_DATE,
        HASH(
            JOB_ID,
            TITLE,
            COMPANY,
            CATEGORY,
            URL,
            JOB_TYPE,
            PUBLISHED_AT
        )                               AS META_HASH,
        HASH(JOB_ID)                    AS META_BUSINESS_KEY_HASH,
        0                               AS META_IS_DELETED

    FROM JOBS.STA.JOBS_RAW

),

LATEST_RECORDS AS (
    SELECT *
    FROM JOBS.DWH.JOBS_BASE
),

FILTERED_LATEST AS (
    SELECT *
    FROM LATEST_RECORDS
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY META_BUSINESS_KEY_HASH
        ORDER BY META_INSERT_DATE DESC
    ) = 1
    AND META_IS_DELETED = 0
),

DELETED_RECORDS AS (
    SELECT
        L.*
    FROM FILTERED_LATEST L
    LEFT JOIN SOURCE_DATA S
        ON L.META_BUSINESS_KEY_HASH = S.META_BUSINESS_KEY_HASH
    WHERE S.META_BUSINESS_KEY_HASH IS NULL
)

SELECT * 
FROM SOURCE_DATA
WHERE META_BUSINESS_KEY_HASH NOT IN (
    SELECT META_BUSINESS_KEY_HASH FROM LATEST_RECORDS
)

UNION ALL

SELECT
    JOB_ID,
    TITLE,
    COMPANY,
    CATEGORY,
    URL,
    JOB_TYPE,
    PUBLISHED_AT,
    CURRENT_TIMESTAMP()              AS META_INSERT_DATE,
    META_HASH,
    META_BUSINESS_KEY_HASH,
    1                                AS META_IS_DELETED
FROM DELETED_RECORDS
