CREATE MATERIALIZED VIEW IF NOT EXISTS electricity.measurements_latest_mv
TO electricity.measurements_latest
AS
SELECT ts, source, metric, value, ukey, version, inserted_at
FROM electricity.measurements;
