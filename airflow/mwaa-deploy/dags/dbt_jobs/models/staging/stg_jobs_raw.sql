-- models/staging/stg_jobs_raw.sql
select
  parse_json(raw:publication_date) as publication_date,
  parse_json(raw:job_type) as job_type,
  parse_json(raw:title) as title
from jobs.sta.jobs_raw
