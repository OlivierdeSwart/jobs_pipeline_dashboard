{{ config(
    materialized = 'table',
    alias = 'bridge_job_tags',
    tags = ['dm', 'jobs', 'bridge']
) }}

{# 
  Bridge table for job tags. 
  Enables slicing and analysis at the individual tag level by modeling a one-to-many relationship 
  between job postings and their associated tags. Each row represents a single tag for a given job. 
#}

WITH source AS (

    SELECT
        HASH(JOB_ID)                    AS FCT_JOB_POSTING_KEY,
        JOB_ID                          AS JOB_ID,
        LOWER(T.VALUE::STRING)          AS TAG,
        CURRENT_TIMESTAMP()             AS META_INSERT_DATE

    FROM {{ ref('jobs_current') }},
         LATERAL FLATTEN(INPUT => TAGS) AS T

)

SELECT * FROM source