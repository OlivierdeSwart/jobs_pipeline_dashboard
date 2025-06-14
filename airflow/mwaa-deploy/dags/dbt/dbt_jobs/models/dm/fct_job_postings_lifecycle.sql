{{ config(
    materialized = 'table',
    alias = 'fct_job_postings_lifecycle',
    description = 'Fact table for job postings lifecycle. This table captures the lifecycle of job postings, including their active and deleted states. One row per job, both active and archived',
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
        publication_date,
        SALARY,
        TAGS,
        1 AS META_ROW,
        CASE WHEN META_IS_DELETED = 0 THEN DATEDIFF(DAY,publication_date,CURRENT_TIMESTAMP())
                ELSE DATEDIFF(DAY,publication_date,META_INSERT_DATE)
            END AS JOB_OPEN_DAYS,
        META_IS_DELETED::INT                                                                    AS META_IS_DELETED,
        CASE WHEN META_IS_DELETED = 0 AND META_VERSION > 1 THEN 1
             ELSE 0
        end                                                                                     AS META_IS_UPDATED,
        CASE WHEN META_IS_DELETED = 0 AND META_VERSION = 1 THEN 1
             ELSE 0
        end                                                                                     AS META_IS_NEW,
        META_INSERT_DATE                                                                        AS META_INSERT_DATE_DWH,
        CURRENT_TIMESTAMP()                                                                     AS META_INSERT_DATE_FCT
    FROM {{ ref('jobs_versioning') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY meta_business_key_hash
        ORDER BY meta_insert_date DESC
    ) = 1
)

SELECT * FROM SOURCE
