{{ config(
    materialized = 'incremental',
    unique_key = ['job_id', 'meta_insert_date'],
    incremental_strategy = 'append',
    on_schema_change = 'append_new_columns',
    tags = ['dwh', 'jobs'],
    alias = 'jobs_base'
) }}

-- ✅ Extract raw data from the staging table
WITH sta_data AS (

    SELECT
        RAW_DATA:"id"::INT                                AS job_id,
        RAW_DATA:"title"::STRING                          AS title,
        RAW_DATA:"company_name"::STRING                   AS company,
        RAW_DATA:"company_logo"::STRING                   AS company_logo,
        RAW_DATA:"category"::STRING                       AS category,
        RAW_DATA:"url"::STRING                            AS url,
        RAW_DATA:"job_type"::STRING                       AS job_type,
        RAW_DATA:"publication_date"::TIMESTAMP            AS published_at,
        RAW_DATA:"candidate_required_location"::STRING    AS candidate_location,
        RAW_DATA:"salary"::STRING                         AS salary,
        RAW_DATA:"description"::STRING                    AS description,
        RAW_DATA:"tags"::ARRAY                            AS tags,

        -- 🔐 SCD2 metadata
        CURRENT_TIMESTAMP()                               AS meta_insert_date,
        HASH(
            RAW_DATA:"id",
            RAW_DATA:"title",
            RAW_DATA:"company_name",
            RAW_DATA:"category",
            RAW_DATA:"url",
            RAW_DATA:"job_type",
            RAW_DATA:"publication_date",
            RAW_DATA:"candidate_required_location",
            RAW_DATA:"salary",
            RAW_DATA:"description",
            RAW_DATA:"tags"
        )                                                 AS meta_hash,
        HASH(RAW_DATA:"id")                               AS meta_business_key_hash,
        0                                                 AS meta_is_deleted

    FROM JOBS.STA.JOBS_RAW

),

-- ✅ Get latest active records from DWH
dwh_latest_active_only AS (

    SELECT *
    FROM {{ this }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY meta_business_key_hash
        ORDER BY meta_insert_date DESC
    ) = 1
    AND meta_is_deleted = 0

),

-- ❌ Deleted: no longer present in STA
deleted_records AS (

    SELECT
        d.*
    FROM dwh_latest_active_only d
    LEFT JOIN sta_data s
        ON d.meta_business_key_hash = s.meta_business_key_hash
    WHERE s.meta_business_key_hash IS NULL

)

-- ✅ New or changed rows only
SELECT s.*
FROM sta_data s
LEFT JOIN dwh_latest_active_only d ON s.meta_hash = d.meta_hash
WHERE d.meta_hash IS NULL

UNION ALL

-- ❌ Deleted records
SELECT
    job_id,
    title,
    company,
    company_logo,
    category,
    url,
    job_type,
    published_at,
    candidate_location,
    salary,
    description,
    tags,
    CURRENT_TIMESTAMP()              AS meta_insert_date,
    meta_hash,
    meta_business_key_hash,
    1                                AS meta_is_deleted
FROM deleted_records