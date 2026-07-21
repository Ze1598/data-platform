"""Generic, source-type-agnostic schema discovery: given a DataFrame,
produce a fresh schema_registry-shaped column_definitions list. Shared by
every connector kind/case that discovers schema by inference rather than
an authoritative catalog lookup (CSV, JSON/REST's default
discover_schema(), and a Postgres feed using a custom multi-table query --
PostgresConnector.discover_schema() queries pg_catalog directly instead for
the single-real-table case, see connectors/postgres.py).

Generalized out of what raw_to_clean.schema_evolution.reconcile_schema()
used to do inline (its "new column" branch) -- this only ever produces a
*fresh* discovery result; deciding whether it represents a change worth
writing to schema_registry is connectors.schema_registry_sync's job.

A column whose fetched data is entirely null this run gets data_type=None
here -- a sentinel meaning "no data to infer from," not a guess. Resolving
that (default to "string" for a genuinely new column, or keep whatever
schema_registry already has for an existing one) needs the current
registry state, which this function doesn't have -- that's
schema_registry_sync.compute_schema_sync()'s job.
"""

from typing import Any

import polars as pl

from raw_to_clean.schema_validation import TYPE_MAP

_REVERSE_TYPE_MAP: dict[type, str] = {v: k for k, v in TYPE_MAP.items()}


def infer_column_definitions(df: pl.DataFrame) -> list[dict[str, Any]]:
    definitions = []
    for ordinal, name in enumerate(df.columns):
        dtype = df.schema[name].base_type()
        data_type = None if dtype == pl.Null else _resolve_data_type(name, dtype)
        nullable = df[name].null_count() > 0
        definitions.append({"name": name, "data_type": data_type, "nullable": nullable, "ordinal": ordinal})
    return definitions


def _resolve_data_type(column_name: str, dtype: type) -> str:
    data_type = _REVERSE_TYPE_MAP.get(dtype)
    if data_type is None:
        raise ValueError(
            f"column '{column_name}': unsupported inferred Polars dtype {dtype}, no schema_registry data_type mapping"
        )
    return data_type
