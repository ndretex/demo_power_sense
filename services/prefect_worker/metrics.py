"""Deprecated metrics module (Prometheus metrics disabled)."""


def start_metrics_server(*_args, **_kwargs) -> None:
    return


class _NoopMetric:
    def inc(self, *_args, **_kwargs) -> None:
        return

    def set(self, *_args, **_kwargs) -> None:
        return


ingest_rows_written_total = _NoopMetric()
ingest_errors_total = _NoopMetric()
ingest_last_success_timestamp = _NoopMetric()
