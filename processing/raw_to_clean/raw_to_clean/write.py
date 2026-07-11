from typing import Any

import polars as pl
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import BooleanType, DoubleType, LongType, NestedField, StringType, TimestamptzType

_ICEBERG_TYPE_MAP = {
    "string": StringType,
    "long": LongType,
    "double": DoubleType,
    "boolean": BooleanType,
    # Always tz-aware: every timestamp this pipeline generates comes from
    # datetime.now(timezone.utc) (see extraction_assets.py/sales_assets.py),
    # which Polars' .to_arrow() represents as a tz-aware Arrow timestamp —
    # PyIceberg's schema check treats that as a strictly different type
    # from a naive TimestampType and rejects the write otherwise.
    "timestamp": TimestamptzType,
}


def _schema_from_column_definitions(column_definitions: list[dict[str, Any]]) -> Schema:
    # Always nullable at the Iceberg level, regardless of schema_registry's
    # `nullable` flag: Polars' .to_arrow() always produces nullable Arrow
    # fields, even for a column with zero actual nulls, so a `required`
    # Iceberg field rejects every write from this path. `nullable: false`
    # is enforced instead where it belongs — as a real data-quality check,
    # in validate_schema() below, which runs before this is ever called.
    fields = [
        NestedField(
            field_id=col["ordinal"],
            name=col["name"],
            field_type=_ICEBERG_TYPE_MAP[col["data_type"]](),
            required=False,
        )
        for col in sorted(column_definitions, key=lambda c: c["ordinal"])
    ]
    return Schema(*fields)


def write_clean_snapshot(
    catalog: Catalog,
    *,
    namespace: str,
    table_name: str,
    df: pl.DataFrame,
    column_definitions: list[dict[str, Any]],
    schema_changed: bool = False,
) -> Table:
    """Writes `df` as `namespace.table_name`'s entire current content — one
    atomic commit (pyiceberg Table.overwrite), not a delete-then-insert
    pair. This is what makes "clean is a full snapshot per run" (Roadmap.md,
    "Layer Model") actually safe under the concurrency pools set up in
    Phase 5, not just pool-protected. Creates the table on first run using
    the current schema_registry definition; every later run just overwrites.

    `schema_changed=True` (set when raw_to_clean.reconcile_schema() returns
    a non-None updated_column_definitions) drops and recreates the table
    against the new schema instead of overwriting in place. `clean` never
    retains history across runs — it's a snapshot of just this run's batch,
    nothing downstream reads an old clean snapshot once a new one lands —
    so there's nothing to lose by recreating it; this sidesteps Iceberg's
    schema evolution rules entirely (which don't allow arbitrary type
    changes like string→long in place, only a narrow set of "safe"
    promotions) rather than needing an in-place-evolve-with-fallback path.
    """
    identifier = (namespace, table_name)
    if schema_changed and catalog.table_exists(identifier):
        catalog.drop_table(identifier)

    if not catalog.table_exists(identifier):
        # Every prior test this session silently relied on `clean` already
        # existing as a namespace, left over from Phase 3's original manual
        # setup — never hit on a genuinely fresh catalog until a full
        # cluster rebuild exposed it. create_table() requires the
        # namespace to exist first; it doesn't create one implicitly.
        catalog.create_namespace_if_not_exists(namespace)
        schema = _schema_from_column_definitions(column_definitions)
        table = catalog.create_table(identifier, schema=schema)
    else:
        table = catalog.load_table(identifier)

    table.overwrite(df.to_arrow())
    return table
