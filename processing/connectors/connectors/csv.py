"""File-drop CSV connector -- reads whatever's currently sitting in a
landing directory. Watermark-based filtering (which files are "new") is
deliberately NOT this connector's job: that needs data_feed.
last_watermark_value, which is orchestration state, not "how do I read a
CSV" mechanics -- the caller filters fetch()'s output.
"""

from pathlib import Path

import polars as pl

from connectors.base import TabularConnector


class CSVConnector(TabularConnector):
    def __init__(self, landing_dir: Path):
        self._landing_dir = landing_dir

    def fetch(self) -> pl.DataFrame:
        if not self._landing_dir.exists():
            return pl.DataFrame()
        csv_files = sorted(self._landing_dir.glob("*.csv"))
        if not csv_files:
            return pl.DataFrame()
        # diagonal_relaxed, not vertical_relaxed -- successive batches can
        # genuinely differ in column set over time (schema evolution),
        # and vertical_relaxed only tolerates differing dtypes for an
        # *identical* column set, erroring outright the moment one file
        # has an extra or missing column. diagonal_relaxed unions the
        # columns across files and fills a file that lacks one with null.
        # Whatever type Polars infers for each column (a date/timestamp
        # column included) is exactly what schema discovery reports and
        # schema_registry records -- schema inference isn't expected to be
        # 100% correct, it reflects the engine's actual capability. Any
        # casting to a "real" type (e.g. string -> timestamp, normalized
        # to UTC) is staging's job, not extraction's -- schema_registry is
        # the source of truth for raw/clean, not for staging forward (see
        # stg_<feed>.sql's own explicit-cast pattern).
        return pl.concat(
            [pl.read_csv(f, infer_schema_length=None) for f in csv_files], how="diagonal_relaxed"
        )
