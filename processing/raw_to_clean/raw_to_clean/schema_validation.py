from typing import Any

import polars as pl

# schema_registry.column_definitions data_type -> Polars dtype, per
# Roadmap.md "Metadata Schema". Kept deliberately small and Iceberg-shaped
# (not every Polars dtype) since these are also the types PyIceberg tables
# in `clean` are created with.
_TYPE_MAP: dict[str, type] = {
    "string": pl.Utf8,
    "long": pl.Int64,
    "double": pl.Float64,
    "boolean": pl.Boolean,
    "timestamp": pl.Datetime,
}


class SchemaValidationError(Exception):
    """Raised when a DataFrame doesn't match a feed's current
    schema_registry definition. Never caught silently — this is meant to
    fail the Dagster run and land in run_audit_log as a real failure."""


def validate_schema(df: pl.DataFrame, column_definitions: list[dict[str, Any]]) -> None:
    errors: list[str] = []
    expected_by_name = {c["name"]: c for c in column_definitions}
    actual_columns = set(df.columns)
    expected_columns = set(expected_by_name)

    missing = expected_columns - actual_columns
    if missing:
        errors.append(f"missing required columns: {sorted(missing)}")

    unexpected = actual_columns - expected_columns
    if unexpected:
        errors.append(f"unexpected columns not in schema_registry: {sorted(unexpected)}")

    for name, col_def in expected_by_name.items():
        if name not in actual_columns:
            continue

        expected_dtype = _TYPE_MAP.get(col_def["data_type"])
        if expected_dtype is None:
            errors.append(f"column '{name}': unknown schema_registry data_type {col_def['data_type']!r}")
        elif df.schema[name].base_type() != expected_dtype:
            errors.append(
                f"column '{name}': expected {col_def['data_type']} ({expected_dtype}), got {df.schema[name]}"
            )

        if not col_def.get("nullable", True):
            null_count = df[name].null_count()
            if null_count > 0:
                errors.append(f"column '{name}': {null_count} null value(s) but schema_registry marks it non-nullable")

    if errors:
        raise SchemaValidationError("; ".join(errors))
