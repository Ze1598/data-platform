"""File-drop JSON connector -- reads whatever nested JSON files are
currently sitting in a landing directory (the second nested-JSON source
shape alongside RestConnector). flatten() stays abstract, same reasoning
as RestConnector: the nested shape is source-specific, no generic
flattening is possible. Watermark filtering is the caller's job, same as
CSVConnector.
"""

from pathlib import Path

import polars as pl

from connectors.base import JsonConnector


class JsonFileConnector(JsonConnector):
    def __init__(self, landing_dir: Path):
        self._landing_dir = landing_dir

    def fetch(self) -> pl.DataFrame:
        if not self._landing_dir.exists():
            return pl.DataFrame()
        json_files = sorted(self._landing_dir.glob("*.json"))
        if not json_files:
            return pl.DataFrame()
        return pl.concat(
            [pl.read_json(f) for f in json_files], how="diagonal_relaxed"
        )

    def flatten(self, raw: pl.DataFrame) -> pl.DataFrame:
        raise NotImplementedError("JsonFileConnector subclasses must implement flatten() -- nested shape is source-specific")
