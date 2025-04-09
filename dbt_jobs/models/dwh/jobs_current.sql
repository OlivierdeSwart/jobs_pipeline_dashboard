{{ config(
    materialized = 'view',
    tags = ['dwh', 'jobs'],
    alias = 'jobs_current'
) }}

SELECT *
FROM {{ ref('jobs_base') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY meta_business_key_hash
    ORDER BY meta_insert_date DESC
) = 1
AND META_IS_DELETED = 0
