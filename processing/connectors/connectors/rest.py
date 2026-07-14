"""REST/JSON-API source connector base. `fetch()` and `flatten()` both
stay abstract -- pagination/date-windowing is as source-specific as
flattening is (police_crimes' month-based catch-up loop has no generic
equivalent), so this only supplies the one genuinely shared piece: a
base-URL-relative GET helper. Per-feed subclasses (e.g.
PoliceCrimesConnector) implement fetch() using `_get()`.
"""

from typing import Any, Optional

import polars as pl
import requests

from connectors.base import JsonConnector


class RestConnector(JsonConnector):
    def __init__(self, *, base_url: str, timeout: int = 60):
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    def _get(self, path: str, params: Optional[dict[str, Any]] = None) -> Any:
        resp = requests.get(f"{self._base_url}/{path.lstrip('/')}", params=params, timeout=self._timeout)
        resp.raise_for_status()
        return resp.json()

    def fetch(self) -> pl.DataFrame:
        raise NotImplementedError("RestConnector subclasses must implement fetch() -- pagination is source-specific")

    def flatten(self, raw: pl.DataFrame) -> pl.DataFrame:
        raise NotImplementedError("RestConnector subclasses must implement flatten() -- response shape is source-specific")
