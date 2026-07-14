from typing import Any

import polars as pl

from raw_to_clean.schema_validation import TYPE_MAP


class MissingColumnsError(Exception):
    """Raised when a column present in schema_registry's current version is
    absent from this run's incoming data -- always fails the run. Scoped to
    just this feed and its downstream dependents by the existing
    per-feed pool/error-propagation machinery (log_data_feed_stage already
    logs-and-reraises on any exception), no separate isolation mechanism
    needed."""


def reconcile_schema(df: pl.DataFrame, column_definitions: list[dict[str, Any]]) -> pl.DataFrame:
    """Coerces `df` (as constructed by an extraction connector) to match a
    feed's current, already-established schema_registry contract -- pure
    df -> df coercion, this function never writes to schema_registry.
    Schema *discovery* (deciding whether the contract itself needs to
    change) runs earlier, at extraction time, via
    connectors.schema_registry_sync.compute_schema_sync() -- see the
    connector library plan. Two cases, both properties of *this run's
    batch* against an already-known contract, not schema facts:

    - A registered column is all-null this run: cast to the already-known
      type -- no new information to act on.
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

    for name in actual_columns:
        col_def = expected_by_name.get(name)
        if col_def is None:
            # Not yet in schema_registry -- schema discovery (run before
            # this, at extraction time) should already have added it via
            # compute_schema_sync(). If it hasn't, that's a caller bug,
            # not something this function should paper over.
            raise ValueError(
                f"column '{name}' is not in schema_registry and was not added by schema discovery -- "
                "the extraction stage's discover_schema()/compute_schema_sync() call was likely skipped"
            )

        expected_dtype = TYPE_MAP.get(col_def["data_type"])
        if expected_dtype is None:
            raise ValueError(f"column '{name}': unknown schema_registry data_type {col_def['data_type']!r}")

        actual_dtype = df.schema[name].base_type()
        if actual_dtype == pl.Null:
            # Nothing but nulls this run -- cast to the already-known type,
            # not a schema change; we have no new information to act on.
            df = df.with_columns(pl.col(name).cast(expected_dtype))
        # Any other mismatch is validate_schema()'s job to report -- schema
        # discovery already had its chance to reconcile a legitimate type
        # change before this function ever ran.

    return df


def parse_ddl_schema(ddl: str) -> dict[str, type]:
    """Optional Spark-DDL-style schema override, e.g.
    "account_code string, amount double" -- comma-separated
    "<column> <type>" pairs, using Polars' own type-name vocabulary
    directly (resolved via getattr(pl, type_name)), no translation layer.
    For call sites that want to force a column's type at construction time
    (pl.read_csv(schema_overrides=...), pl.DataFrame(schema=...)) instead
    of letting schema discovery infer it -- most useful the first time a
    feed lands, before any schema_registry history exists.
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
