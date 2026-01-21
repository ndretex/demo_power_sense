"""Deprecated shim. Use ingestion.transform instead."""

from ingestion.transform import normalize_record, remap_metric_name

__all__ = ["normalize_record", "remap_metric_name"]
