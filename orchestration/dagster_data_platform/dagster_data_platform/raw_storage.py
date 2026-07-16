"""Shared read/write helpers for the `raw` schema-stage's durable parquet
files -- the one piece of the extraction job's own internal raw -> clean
handoff that needs to go through real storage rather than an in-memory
Dagster asset-dependency value, per the same "read the previous layer's
storage, never pass a data variable across" rule applied to every other
layer boundary (clean -> staging is already storage-based for free, since
dbt reads Iceberg tables; this is the one boundary that used to be an
in-memory pl.DataFrame parameter). See Roadmap.md "Master pipeline
orchestration".

Keyed by the extraction job's own `context.run_id`, not
`master_dagster_run_id` -- raw_<feed> and clean_<feed> are steps within the
exact same job run (EXTRACTION_JOBS[feed] bundles raw+clean into one job/
pod), so `context.run_id` is already identical between the writer and the
reader; there is no cross-run lookup to do here.
"""

import os
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[3]


def _data_lake_dir() -> Path:
    return Path(os.environ["DATA_LAKE_PATH"]) if "DATA_LAKE_PATH" in os.environ else REPO_ROOT / "data-lake"


def raw_snapshot_path(feed_friendly_name: str, run_id: str) -> Path:
    return _data_lake_dir() / "raw" / feed_friendly_name / f"run_id={run_id}" / f"{feed_friendly_name}.parquet"


def write_raw_snapshot(feed_friendly_name: str, run_id: str, df: pl.DataFrame) -> None:
    if df.is_empty():
        return
    path = raw_snapshot_path(feed_friendly_name, run_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def read_raw_snapshot(feed_friendly_name: str, run_id: str) -> pl.DataFrame:
    path = raw_snapshot_path(feed_friendly_name, run_id)
    if not path.exists():
        return pl.DataFrame()
    return pl.read_parquet(path)
