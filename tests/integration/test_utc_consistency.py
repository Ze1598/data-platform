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
        SELECT df.code, df.staging_table_name, sr.column_definitions
        FROM schema_registry sr
        JOIN data_feed df ON df.id = sr.data_feed_id
        WHERE sr.is_current AND df.is_active
        """
    )
    for code, staging_table_name, column_definitions in cur.fetchall():
        timestamp_columns = [c["name"] for c in column_definitions if c["data_type"] == "timestamp"]
        if timestamp_columns:
            yield code, staging_table_name, timestamp_columns


def test_clean_layer_timestamps_are_timezone_aware(trino_conn, metadata_conn):
    feeds = list(_current_feeds_with_timestamp_columns(metadata_conn))
    assert feeds, "expected at least one feed with a timestamp column in schema_registry — did seeding run?"

    failures = []
    for feed_code, _staging_table_name, timestamp_columns in feeds:
        columns = describe_columns(trino_conn, "clean", feed_code)
        assert columns, f"iceberg.clean.{feed_code} does not exist — has this feed ever been materialized?"
        for col in timestamp_columns:
            trino_type = columns.get(col)
            if trino_type is None or "with time zone" not in trino_type:
                failures.append(f"clean.{feed_code}.{col}: expected 'timestamp(...) with time zone', got {trino_type!r}")

    assert not failures, "\n".join(failures)


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
    for feed_code, staging_table_name, timestamp_columns in feeds:
        columns = describe_columns(trino_conn, "staging", staging_table_name)
        assert columns, f"iceberg.staging.{staging_table_name} does not exist — has stg_{feed_code} ever run?"
        # the feed's own passed-through column(s), plus the technical
        # _loaded_at column every staging model stamps (see
        # dbt/data_platform/models/staging/stg_*.sql)
        for col in [*timestamp_columns, "_loaded_at"]:
            trino_type = columns.get(col)
            if trino_type is None or "with time zone" not in trino_type:
                failures.append(
                    f"staging.{staging_table_name}.{col}: expected 'timestamp(...) with time zone', got {trino_type!r}"
                )

    assert not failures, "\n".join(failures)
