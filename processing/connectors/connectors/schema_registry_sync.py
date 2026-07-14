"""Diffs a freshly discovered schema against schema_registry's current
state and decides whether it represents a change worth writing. Pure,
side-effect-free -- deliberately doesn't take a PostgresMetadataResource
or do any I/O itself (this package has no dependency on
orchestration/Dagster, same as raw_to_clean). The caller (a Dagster asset,
hand-written or codegen'd) is responsible for:

    1. discovered = connector.discover_schema(df)
    2. current = try postgres_metadata.get_current_schema(data_feed_id),
       catching its ValueError as None (no registry yet -- first run)
    3. result = compute_schema_sync(discovered, current)
    4. if result.changed: postgres_metadata.update_schema_registry(
           data_feed_id=..., column_definitions=result.column_definitions,
           created_by=...)
    5. use result.column_definitions as the now-current contract for
       raw_to_clean.reconcile_schema()/validate_schema() and for
       write_clean_snapshot()'s schema_changed= flag.

This replaces raw_to_clean.schema_evolution.reconcile_schema()'s former
"new column" / "type changed" evolution branches -- those were schema
*discovery* concerns entangled with validation-time coercion. Discovery
now runs once per extraction, before validation ever executes.
"""

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class SchemaSyncResult:
    column_definitions: list[dict[str, Any]]
    changed: bool


def compute_schema_sync(
    discovered_column_definitions: list[dict[str, Any]],
    current_column_definitions: Optional[list[dict[str, Any]]],
) -> SchemaSyncResult:
    """Two cases, mirroring the evolution rules formerly in
    reconcile_schema():

    - No current registry entry (`current_column_definitions is None`):
      first-time bootstrap -- the discovered schema becomes current
      as-is.
    - A registry entry exists: a genuinely new column, or an existing
      column with a different data_type than registered, is a legitimate
      upstream schema change -- merged into the current list (new columns
      appended at the next ordinal, changed types updated in place) and
      `changed=True`. A column in the current registry but absent from
      this run's *discovery* is left untouched here -- a feed's schema
      registry only ever grows/updates via discovery, it never shrinks;
      a column disappearing from actual data is
      raw_to_clean.schema_evolution.MissingColumnsError's job to catch,
      at validation time.
    """
    if current_column_definitions is None:
        return SchemaSyncResult(column_definitions=discovered_column_definitions, changed=True)

    current_by_name = {c["name"]: c for c in current_column_definitions}
    discovered_by_name = {c["name"]: c for c in discovered_column_definitions}

    merged = [dict(c) for c in current_column_definitions]
    next_ordinal = max((c["ordinal"] for c in merged), default=-1) + 1
    changed = False

    for name, discovered_col in discovered_by_name.items():
        current_col = current_by_name.get(name)
        if current_col is None:
            merged.append({**discovered_col, "ordinal": next_ordinal})
            next_ordinal += 1
            changed = True
        elif current_col["data_type"] != discovered_col["data_type"]:
            for c in merged:
                if c["name"] == name:
                    c["data_type"] = discovered_col["data_type"]
            changed = True

    return SchemaSyncResult(column_definitions=merged, changed=changed)
