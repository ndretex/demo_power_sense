{{ config(alias='measurements_latest') }}

with latest_per_ukey as (
  select
    ukey,
    max(tuple(version, inserted_at, ts, source, metric, value)) as latest_row
  from {{ ref('stg_electricity__measurements') }}
  group by ukey
)
select
  tupleElement(latest_row, 3) as ts,
  tupleElement(latest_row, 4) as source,
  tupleElement(latest_row, 5) as metric,
  tupleElement(latest_row, 6) as value,
  ukey,
  tupleElement(latest_row, 1) as version,
  tupleElement(latest_row, 2) as inserted_at
from latest_per_ukey
