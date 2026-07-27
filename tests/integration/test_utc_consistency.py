"""Regression test for the timestamp-timezone bug found in Phase 6:
clean_customers wrote naive `timestamp(6)` (via a raw Trino literal) while
clean_sales correctly wrote tz-aware `timestamp(6) with time zone` (via
PyIceberg's TimestamptzType) — for the same logical kind of column,
different feeds, different code paths. See Learnings.md, Phase 6.

Every `data_type: "timestamp"` column in `schema_registry` is supposed to
represent a real instant (always generated via `datetime.now(timezone.utc)`
in this project's asset code — see extraction_assets.py/sales_assets.py),
so every such column must be `with time zone` everywhere beyond raw:
clean, staging, and (once Phase 7 exists) model/serve. This test is
metadata-driven — it checks whatever feeds schema_registry currently
knows about, not a hardcoded list, so it keeps working as feeds are added.
"""

import pytest

from conftest import describe_columns


def _current_feeds_with_timestamp_columns(metadata_conn):
    cur = metadata_conn.cursor()
    cur.execute(
        """
        SELECT df.friendly_name, sr.column_definitions
        FROM schema_registry sr
        JOIN data_feed df ON df.id = sr.controlling_object_id AND sr.controlling_object_type = 'feed'
        WHERE sr.is_current AND df.is_active
        """
    )
    for friendly_name, column_definitions in cur.fetchall():
        timestamp_columns = [c["name"] for c in column_definitions if c["data_type"] == "timestamp"]
        if timestamp_columns:
            yield friendly_name, timestamp_columns


def test_clean_layer_timestamps_are_timezone_aware(trino_conn, metadata_conn):
    feeds = list(_current_feeds_with_timestamp_columns(metadata_conn))
    assert feeds, "expected at least one feed with a timestamp column in schema_registry — did seeding run?"

    failures = []
    for friendly_name, timestamp_columns in feeds:
        columns = describe_columns(trino_conn, "clean", friendly_name)
        assert columns, f"iceberg.clean.{friendly_name} does not exist — has this feed ever been materialized?"
        for col in timestamp_columns:
            trino_type = columns.get(col)
            if trino_type is None or "with time zone" not in trino_type:
                failures.append(f"clean.{friendly_name}.{col}: expected 'timestamp(...) with time zone', got {trino_type!r}")

    assert not failures, "\n".join(failures)


def _staging_table_names_for_feed(metadata_cur, friendly_name: str) -> list[str]:
    """Every staging table this feed's clean-layer data can physically land
    in. Historically exactly one, aliased by the feed's own friendly_name
    (every hand-written stg_<feed>.sql, e.g. stg_customers.sql/
    stg_sales.sql -- still true for any feed with no lakehouse_model_columns
    rows). A feed with lakehouse_model_columns rows
    (frontend/pages/7_Model_Table_Columns.py) also gets one DEDICATED
    stg_<table_name> per model sourcing from it, aliased by that model's
    table_name -- NOT the feed's own name (see
    scripts/generate_model_scaffolds.py's dedicated-staging codegen). A
    feed can have both at once (a pre-existing hand-written staging model
    plus a newly-added dedicated one for a different model), so this
    returns every structurally-possible name; the caller checks whichever
    ones actually exist."""
    metadata_cur.execute(
        """
        SELECT DISTINCT lm.table_name
        FROM lakehouse_model_columns lmc
        JOIN data_feed df ON df.id = lmc.source_feed_id
        JOIN lakehouse_models lm ON lm.id = lmc.model_id
        WHERE df.friendly_name = %s
        """,
        (friendly_name,),
    )
    return [friendly_name] + [row[0] for row in metadata_cur.fetchall()]


def test_staging_layer_timestamps_are_timezone_aware(trino_conn, metadata_conn):
    """Covers the second half of the actual bug: even after clean.customers
    was fixed, a pre-existing staging.customers table stayed naive, because
    dbt's incremental MERGE doesn't change an existing table's column
    types — only a fresh CREATE TABLE AS SELECT infers the corrected type.
    A stale table failing this check is the real-world failure mode, not
    a hypothetical one."""
    feeds = list(_current_feeds_with_timestamp_columns(metadata_conn))
    assert feeds, "expected at least one feed with a timestamp column in schema_registry — did seeding run?"

    failures = []
    metadata_cur = metadata_conn.cursor()
    for friendly_name, timestamp_columns in feeds:
        staging_table_names = _staging_table_names_for_feed(metadata_cur, friendly_name)
        found_any = False
        for table_name in staging_table_names:
            columns = describe_columns(trino_conn, "staging", table_name)
            if not columns:
                continue
            found_any = True
            # schema_registry reflects clean's full discovered schema, which
            # can be wider than what a staging model actually selects (e.g.
            # a feed extracted via a generic `SELECT *` -- see
            # metadata/DataModel.md, data_feed.extraction_config) --
            # curating columns down in staging is normal, intended dbt
            # modeling, not a gap, so only check timestamp columns staging
            # actually carries through, plus the technical _loaded_at
            # column every staging model stamps.
            for col in [*timestamp_columns, "_loaded_at"]:
                if col not in columns:
                    continue
                trino_type = columns[col]
                if "with time zone" not in trino_type:
                    failures.append(
                        f"staging.{table_name}.{col}: expected 'timestamp(...) with time zone', got {trino_type!r}"
                    )
        assert found_any, (
            f"none of feed {friendly_name!r}'s plausible staging tables exist yet "
            f"({', '.join(staging_table_names)}) — has its staging model ever run?"
        )

    assert not failures, "\n".join(failures)
