{{ config(
    materialized = 'view',
    tags = ['dwh', 'jobs'],
    alias = 'jobs_versioning'
) }}

SELECT 
    *,

    -- SCD2 Validity
    meta_insert_date AS meta_valid_from,
    NVL(
        DATEADD(DAY, -1, 
            LAG(meta_insert_date) OVER (
                PARTITION BY meta_business_key_hash 
                ORDER BY meta_insert_date DESC
            )
        ),
        TO_TIMESTAMP('2999-12-31 23:59:59.999')
    ) AS meta_valid_to,

    -- Current flag
    DECODE(
        ROW_NUMBER() OVER (
            PARTITION BY meta_business_key_hash 
            ORDER BY meta_insert_date DESC
        ), 
        1, 1, 0
    ) AS meta_is_current,

    -- Version number
    ROW_NUMBER() OVER (
        PARTITION BY meta_business_key_hash 
        ORDER BY meta_insert_date ASC
    ) AS meta_version

FROM {{ ref('jobs_base') }}