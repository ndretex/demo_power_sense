with bounds as (
  select
    ifNull(min(toDate(ts)), today()) as min_day,
    greatest(ifNull(max(toDate(ts)), today()), today()) as max_day
  from {{ ref('stg_electricity__measurements') }}
),
date_spine as (
  select
    addDays(min_day, day_offset) as data_day
  from bounds
  array join range(dateDiff('day', min_day, max_day) + 1) as day_offset
),
source_metric as (
  select distinct
    source,
    metric
  from {{ ref('stg_electricity__measurements') }}
),
expected_days as (
  select
    ds.data_day,
    sm.source,
    sm.metric
  from date_spine ds
  cross join source_metric sm
),
actual_daily as (
  select
    toDate(ts) as data_day,
    source,
    metric,
    count() as rows_ingested,
    min(inserted_at) as first_ingested_at,
    max(inserted_at) as last_ingested_at
  from {{ ref('stg_electricity__measurements') }}
  group by data_day, source, metric
)
select
  e.data_day,
  e.source,
  e.metric,
  coalesce(a.rows_ingested, 0) as rows_ingested,
  a.first_ingested_at,
  a.last_ingested_at,
  coalesce(a.rows_ingested, 0) > 0 as has_data,
  coalesce(a.rows_ingested, 0) = 0 as is_missing_day
from expected_days e
left join actual_daily a
  on e.data_day = a.data_day
 and e.source = a.source
 and e.metric = a.metric
