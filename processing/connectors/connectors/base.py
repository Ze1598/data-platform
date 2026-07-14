"""Two connector shapes, not one uniform interface -- tabular and
nested-JSON sources genuinely behave differently for schema discovery
(see the connector library plan in .claude/plans/, and Learnings.md).
"""

from abc import ABC, abstractmethod
from typing import Any

import polars as pl

from connectors.inference import infer_column_definitions


class TabularConnector(ABC):
    """A source whose data is already flat (Postgres query results, CSV
    files) -- extraction and validation stay two genuinely separate
    stages for this shape: extraction fetches, discovers schema, and
    writes the verbatim raw copy; a separate, fully generic validation
    step checks against the registry and writes clean."""

    @abstractmethod
    def fetch(self) -> pl.DataFrame:
        """Reads/queries the source, returns an already-flat DataFrame."""

    def discover_schema(self, df: pl.DataFrame) -> list[dict[str, Any]]:
        """Returns a fresh schema_registry-shaped column_definitions list
        for `df`. Default: generic sample-inference. Override for a
        source with an authoritative catalog to query instead (e.g. a
        single real Postgres table's information_schema)."""
        return infer_column_definitions(df)


class JsonConnector(ABC):
    """A source whose data arrives nested (REST API responses, JSON file
    drops). Flattening is inseparable from establishing the real (flat)
    schema contract for this shape -- there's no clean way to discover a
    tabular schema from unflattened structs. So extraction and validation
    combine into one stage for these connectors: fetch (raw, nested,
    verbatim to `raw`) is still separate, but flatten + discover + sync +
    validate + write-to-clean all happen together, driven by whichever
    per-feed subclass supplies flatten() (genuinely bespoke -- no generic
    implementation is possible, since it depends on this source's
    specific nested shape)."""

    @abstractmethod
    def fetch(self) -> pl.DataFrame:
        """Reads/queries the source, returns the raw nested rows (struct
        columns) -- what raw storage gets, verbatim, zero
        transformation."""

    @abstractmethod
    def flatten(self, raw: pl.DataFrame) -> pl.DataFrame:
        """Flattens `raw` into the flat row shape schema_registry
        expects. Must be implemented per feed."""

    def discover_schema(self, flat: pl.DataFrame) -> list[dict[str, Any]]:
        """Default: generic sample-inference over the already-flattened
        DataFrame. Override only if a source can do better (e.g. a
        published OpenAPI/JSON-Schema spec)."""
        return infer_column_definitions(flat)
