CREATE TABLE IF NOT EXISTS electricity.measurements (
  ts          DateTime64(3, 'UTC'),
  source      String,
  metric      String,
  value       Nullable(Float64),
  ukey        String,
  version     UInt64,
  inserted_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (ukey, version);

CREATE TABLE IF NOT EXISTS electricity.anomalies (
  ts          DateTime64(3, 'UTC'),
  source      String,
  metric      String,
  value       Nullable(Float64),
  zscore      Float64,
  mean        Float64,
  std         Float64,
  threshold   Float64,
  dow         UInt8,
  hour        UInt8,
  minute      UInt8,
  inserted_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (metric, ts, source);
