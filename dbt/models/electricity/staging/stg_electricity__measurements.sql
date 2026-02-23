select
  ts,
  source,
  metric,
  value,
  ukey,
  version,
  inserted_at
from {{ source('electricity_raw', 'measurements') }}

