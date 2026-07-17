"""Shared read/write helpers for the `raw` schema-stage's durable parquet
files -- the one piece of the extraction job's own internal raw -> clean
handoff that needs to go through real storage rather than an in-memory
Dagster asset-dependency value, per the same "read the previous layer's
storage, never pass a data variable across" rule applied to every other
layer boundary (clean -> staging is already storage-based for free, since
dbt reads Iceberg tables; this is the one boundary that used to be an
in-memory pl.DataFrame parameter). See Roadmap.md "Master pipeline
orchestration".

Keyed by `storage_watermark` -- a `data_processing_runs` column generated
once, at run-creation time, by `PostgresMetadataResource.record_run_started()`
(a `YYYY/MM/DD/HH/MM/SS` path) -- not by the extraction job's own
`context.run_id`. Both raw_<feed> and clean_<feed> read it back via their
own `log_data_feed_stage(...)` call's `IngestionStepLog.storage_watermark`
(populated from the same row `_find_run()` already looks up for the stage
UPDATE, no extra round trip), so the raw read path is pinned by an explicit
value on the run record rather than implicit run_id parity between two
steps of the same job.
"""

import os
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[3]


def _data_lake_dir() -> Path:
    return Path(os.environ["DATA_LAKE_PATH"]) if "DATA_LAKE_PATH" in os.environ else REPO_ROOT / "data-lake"


def raw_snapshot_path(feed_friendly_name: str, storage_watermark: str) -> Path:
    return _data_lake_dir() / "raw" / feed_friendly_name / storage_watermark / f"{feed_friendly_name}.parquet"


def write_raw_snapshot(feed_friendly_name: str, storage_watermark: str, df: pl.DataFrame) -> None:
    if df.is_empty():
        return
    path = raw_snapshot_path(feed_friendly_name, storage_watermark)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def read_raw_snapshot(feed_friendly_name: str, storage_watermark: str) -> pl.DataFrame:
    path = raw_snapshot_path(feed_friendly_name, storage_watermark)
    if not path.exists():
        return pl.DataFrame()
    return pl.read_parquet(path)
