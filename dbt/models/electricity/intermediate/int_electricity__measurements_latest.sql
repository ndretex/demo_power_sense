with ranked_measurements as (
  select
    ts,
    source,
    metric,
    value,
    ukey,
    version,
    inserted_at,
    row_number() over (partition by ukey order by version desc) as rn
  from {{ ref('stg_electricity__measurements') }}
),
final as (
  select
    ts,
    source,
    metric,
    value,
    ukey,
    version,
    inserted_at
  from ranked_measurements
  where rn = 1
)
select
  ts,
  source,
  metric,
  value,
  ukey,
  version,
  inserted_at
from final
