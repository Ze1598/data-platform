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
        source with an authoritative catalog to query instead --
        PostgresConnector does this for the single-real-table case (see
        connectors/postgres.py), falling back to this default only for a
        custom multi-table query."""
        return infer_column_definitions(df)


class JsonConnector(ABC):
    """A source whose data arrives nested (REST API responses, JSON file
    drops). Flattening is inseparable from establishing the real (flat)
    schema contract for this shape -- there's no clean way to discover a
    tabular schema from unflattened structs. So the extraction step absorbs
    the validation step's write entirely for these connectors, rather than
    flattening the source data twice: fetch + flatten + discover + sync +
    reconcile + validate + write-to-clean all happen together in
    extraction, driven by whichever per-feed subclass supplies flatten()
    (genuinely bespoke -- no generic implementation is possible, since it
    depends on this source's specific nested shape). `raw`'s own write
    stays a separate, later step and is unaffected -- it always persists
    the untouched *nested* fetch() result, never the flattened copy.
    Validation (`clean_<feed>`) becomes a pure pass-through for these
    connector kinds, kept only so `clean.<feed>` has a stable AssetKey for
    dbt source lineage -- there's no validation-stage work left for it to
    do. (Schema discovery ownership itself, independent of this
    flatten-once optimization, is always extraction's job for every
    connector kind, tabular or nested -- see Roadmap.md's "Metadata Schema"
    and `Learnings.md`'s "schema_registry ownership" entry.)"""

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
