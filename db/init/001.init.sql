CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS measurements (
  ts          TIMESTAMPTZ NOT NULL,
  source      TEXT        NOT NULL,
  metric      TEXT        NOT NULL,
  value       DOUBLE PRECISION,
  meta        JSONB,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (ts, source, metric)
);

SELECT create_hypertable('measurements', 'ts', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS measurements_metric_ts_idx ON measurements (metric, ts DESC);


-- create view where each metric is a column
