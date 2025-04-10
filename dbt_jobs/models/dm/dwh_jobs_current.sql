{{ config(
    materialized = 'view',
    alias = 'dwh_jobs_current',
    tags = ['dm', 'jobs', 'raw_passthrough']
) }}

SELECT * 
FROM {{ ref('jobs_current') }}
