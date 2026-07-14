"""Generic, source-type-agnostic schema discovery: given a DataFrame,
produce a fresh schema_registry-shaped column_definitions list. Shared by
every connector kind that discovers schema by inference rather than an
authoritative catalog lookup (CSV, JSON/REST's default discover_schema(),
and Postgres when querying something other than a single real table).

Generalized out of what raw_to_clean.schema_evolution.reconcile_schema()
used to do inline (its "new column" branch) -- this only ever produces a
*fresh* discovery result; deciding whether it represents a change worth
writing to schema_registry is connectors.schema_registry_sync's job.
"""

from typing import Any

import polars as pl

from raw_to_clean.schema_validation import TYPE_MAP

_REVERSE_TYPE_MAP: dict[type, str] = {v: k for k, v in TYPE_MAP.items()}
_NULL_DEFAULT_DATA_TYPE = "string"


def infer_column_definitions(df: pl.DataFrame) -> list[dict[str, Any]]:
    definitions = []
    for ordinal, name in enumerate(df.columns):
        dtype = df.schema[name].base_type()
        if dtype == pl.Null:
            data_type = _NULL_DEFAULT_DATA_TYPE
        else:
            data_type = _resolve_data_type(name, dtype)
        definitions.append(
            {"name": name, "data_type": data_type, "nullable": True, "ordinal": ordinal, "description": None}
        )
    return definitions


def _resolve_data_type(column_name: str, dtype: type) -> str:
    data_type = _REVERSE_TYPE_MAP.get(dtype)
    if data_type is None:
        raise ValueError(
            f"column '{column_name}': unsupported inferred Polars dtype {dtype}, no schema_registry data_type mapping"
        )
    return data_type
