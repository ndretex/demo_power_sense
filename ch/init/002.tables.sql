CREATE TABLE IF NOT EXISTS electricity.measurements (
  ts          DateTime64(3, 'UTC'),
  source      String,
  metric      String,
  value       Nullable(Float64),
  ukey        String,
  version     UInt64,
  inserted_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(ts)
ORDER BY (ukey, version);

CREATE TABLE IF NOT EXISTS electricity.measurements_latest (
  ts          DateTime64(3, 'UTC'),
  source      String,
  metric      String,
  value       Nullable(Float64),
  ukey        String,
  version     UInt64,
  inserted_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (ukey);
