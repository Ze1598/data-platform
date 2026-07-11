from dataclasses import dataclass
from typing import Any

import polars as pl

from raw_to_clean.schema_validation import _TYPE_MAP

_REVERSE_TYPE_MAP: dict[type, str] = {v: k for k, v in _TYPE_MAP.items()}
_NULL_DEFAULT_DATA_TYPE = "string"


class MissingColumnsError(Exception):
    """Raised when a column present in schema_registry's current version is
    absent from this run's incoming data -- always fails the run. Scoped to
    just this feed and its downstream dependents by the existing
    per-feed pool/error-propagation machinery (log_data_feed_stage already
    logs-and-reraises on any exception), no separate isolation mechanism
    needed."""


@dataclass
class SchemaReconciliation:
    df: pl.DataFrame
    # None when nothing about the feed's schema changed this run; otherwise
    # the full new column_definitions list, ready to pass to
    # PostgresMetadataResource.update_schema_registry().
    updated_column_definitions: list[dict[str, Any]] | None


def reconcile_schema(df: pl.DataFrame, column_definitions: list[dict[str, Any]]) -> SchemaReconciliation:
    """Reconciles `df` (as constructed by a landing asset) against a feed's
    current schema_registry definition, without ever requiring the caller
    to hand-write a schema override. Three cases, per the Phase 9 schema
    evolution design (Roadmap.md "Metadata Schema"):

    - A registered column is all-null this run (or brand new and all-null):
      cast to the already-known type; if brand new (never registered), no
      prior type to fall back on, so it defaults to string.
    - A registered column has real, non-null data of a *different* type
      than registered (e.g. account_code switching from string to integer
      precision), or a genuinely new column shows up with real data: this
      is a legitimate upstream schema change -- the registry is updated to
      match reality and the run proceeds under the new schema, it doesn't
      fail.
    - A column present in schema_registry is missing from `df` entirely:
      raises MissingColumnsError -- unlike the above, a disappeared column
      is never auto-healed, since silently dropping a promised column
      would corrupt every downstream consumer of it.
    """
    expected_by_name = {c["name"]: c for c in column_definitions}
    actual_columns = list(df.columns)

    missing = set(expected_by_name) - set(actual_columns)
    if missing:
        raise MissingColumnsError(
            f"column(s) present in schema_registry but absent from this run's data: {sorted(missing)}"
        )

    new_definitions = [dict(c) for c in column_definitions]
    next_ordinal = max((c["ordinal"] for c in new_definitions), default=-1) + 1
    schema_changed = False

    for name in actual_columns:
        actual_dtype = df.schema[name].base_type()
        col_def = expected_by_name.get(name)

        if col_def is None:
            # Brand new column, not in schema_registry yet.
            if actual_dtype == pl.Null:
                df = df.with_columns(pl.col(name).cast(pl.Utf8))
                data_type = _NULL_DEFAULT_DATA_TYPE
            else:
                data_type = _resolve_data_type(name, actual_dtype)
            new_definitions.append(
                {"name": name, "data_type": data_type, "nullable": True, "ordinal": next_ordinal, "description": None}
            )
            next_ordinal += 1
            schema_changed = True
            continue

        expected_dtype = _TYPE_MAP.get(col_def["data_type"])
        if expected_dtype is None:
            raise ValueError(f"column '{name}': unknown schema_registry data_type {col_def['data_type']!r}")

        if actual_dtype == expected_dtype:
            continue

        if actual_dtype == pl.Null:
            # Nothing but nulls this run -- cast to the already-known type,
            # not a schema change; we have no new information to act on.
            df = df.with_columns(pl.col(name).cast(expected_dtype))
            continue

        # Concrete, non-null data with a type that no longer matches the
        # registry -- a genuine upstream schema change. Update the
        # registry and let the run proceed under the new type.
        for c in new_definitions:
            if c["name"] == name:
                c["data_type"] = _resolve_data_type(name, actual_dtype)
        schema_changed = True

    return SchemaReconciliation(df=df, updated_column_definitions=new_definitions if schema_changed else None)


def _resolve_data_type(column_name: str, dtype: type) -> str:
    data_type = _REVERSE_TYPE_MAP.get(dtype)
    if data_type is None:
        raise ValueError(f"column '{column_name}': unsupported inferred Polars dtype {dtype}, no schema_registry data_type mapping")
    return data_type


def parse_ddl_schema(ddl: str) -> dict[str, type]:
    """Optional Spark-DDL-style schema override, e.g.
    "account_code string, amount double" -- comma-separated
    "<column> <type>" pairs, using Polars' own type-name vocabulary
    directly (resolved via getattr(pl, type_name)), no translation layer.
    For call sites that want to force a column's type at construction time
    (pl.read_csv(schema_overrides=...), pl.DataFrame(schema=...)) instead
    of letting reconcile_schema() infer/evolve it after the fact -- most
    useful the first time a feed lands, before any schema_registry history
    exists to reconcile against.
    """
    schema: dict[str, type] = {}
    for part in ddl.split(","):
        part = part.strip()
        if not part:
            continue
        name, type_name = part.rsplit(maxsplit=1)
        dtype = getattr(pl, type_name, None)
        if not isinstance(dtype, type) or not issubclass(dtype, pl.DataType):
            raise ValueError(f"unknown Polars type {type_name!r} in schema override {part!r}")
        schema[name.strip()] = dtype
    return schema
