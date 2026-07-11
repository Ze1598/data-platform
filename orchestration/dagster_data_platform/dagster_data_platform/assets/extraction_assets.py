from datetime import datetime, timezone

import polars as pl
from dagster import AssetExecutionContext, Output, asset

from dagster_data_platform.resources.iceberg_resource import IcebergCatalogResource
from dagster_data_platform.resources.postgres_metadata_resource import PostgresMetadataResource
from raw_to_clean import reconcile_schema, validate_schema, write_clean_snapshot

FEED_CODE = "customers"
# One pool per feed, shared across every step that touches its data anywhere
# in the pipeline (this file + dbt_assets.py) — blocks two runs of this
# feed from overlapping, e.g. one run's clean-layer write racing another
# run's staging merge reading clean.customers mid-write. Cross-run only:
# within a single run, asset dependencies already serialize these steps.
# Scoped per-feed rather than one pool for everything so unrelated feeds
# can still run concurrently once there's more than one. Note this doesn't
# yet handle a single @dbt_assets function spanning *multiple* feeds in one
# dbt invocation (not a concern until a second feed's dbt models share this
# dbt_assets function — see Learnings.md).
FEED_POOL = f"feed:{FEED_CODE}"

# Stub landing payload for Phase 5 — Phase 6 replaces this asset chain with
# a real SparkApplication reading actual source data (see Roadmap.md,
# "Spark Operator + real raw->clean"). `email` is stamped with the current
# run's timestamp so every materialization visibly changes something,
# proving data actually flows landing -> raw -> clean -> dbt staging rather
# than each run being a silent no-op.
_BASE_CUSTOMERS = [
    {"customer_id": 1, "name": "Alice"},
    {"customer_id": 2, "name": "Bob Updated"},
    {"customer_id": 3, "name": "Carol"},
    {"customer_id": 4, "name": "Dave"},
    {"customer_id": 5, "name": "Eve"},
]


@asset(pool=FEED_POOL)
def landing_customers(
    context: AssetExecutionContext, postgres_metadata: PostgresMetadataResource
) -> Output[pl.DataFrame]:
    data_feed = postgres_metadata.get_data_feed(FEED_CODE)
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        stage="landing",
        dagster_run_id=context.run_id,
    ) as log:
        now = datetime.now(timezone.utc)
        df = pl.DataFrame(_BASE_CUSTOMERS).with_columns(
            (pl.col("name").str.split(" ").list.first().str.to_lowercase() + "@example.com").alias("email"),
            pl.lit(now).alias("updated_at"),
        )
        log.set_counts(rows_read=df.height)

    return Output(df, metadata={"audit_run_id": log.run_id, "row_count": df.height})


@asset(pool=FEED_POOL)
def raw_customers(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    landing_customers: pl.DataFrame,
) -> Output[pl.DataFrame]:
    data_feed = postgres_metadata.get_data_feed(FEED_CODE)
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        stage="raw",
        dagster_run_id=context.run_id,
    ) as log:
        # Stub: passes the landing payload through unchanged. Phase 6
        # replaces this with a real raw file write + parse/validate step.
        df = landing_customers
        log.set_counts(rows_read=df.height)

    return Output(df, metadata={"audit_run_id": log.run_id, "row_count": df.height})


@asset(pool=FEED_POOL)
def clean_customers(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    iceberg_catalog: IcebergCatalogResource,
    raw_customers: pl.DataFrame,
) -> Output[None]:
    data_feed = postgres_metadata.get_data_feed(FEED_CODE)
    df = raw_customers
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        stage="clean",
        dagster_run_id=context.run_id,
    ) as log:
        # Same real path as clean_sales — PyIceberg for the atomic
        # overwrite (clean is a snapshot per run, not cumulative, Roadmap.md
        # "Layer Model"), Polars for the DataFrame, schema_registry for
        # validation. This used to be a Trino DELETE+INSERT pair (two
        # separate commits, the exact race the Phase 5 concurrency pools
        # exist to guard against) with no schema validation at all — see
        # Learnings.md for why this got migrated onto raw_to_clean instead
        # of staying a special case.
        column_definitions = postgres_metadata.get_current_schema(str(data_feed["id"]))
        reconciliation = reconcile_schema(df, column_definitions)
        df = reconciliation.df
        schema_changed = reconciliation.updated_column_definitions is not None
        if schema_changed:
            postgres_metadata.update_schema_registry(
                data_feed_id=str(data_feed["id"]),
                column_definitions=reconciliation.updated_column_definitions,
                created_by="clean_customers",
            )
            column_definitions = reconciliation.updated_column_definitions

        validate_schema(df, column_definitions)

        catalog = iceberg_catalog.get_catalog()
        write_clean_snapshot(
            catalog,
            namespace="clean",
            table_name="customers",
            df=df,
            column_definitions=column_definitions,
            schema_changed=schema_changed,
        )
        log.set_counts(rows_inserted=df.height)

    return Output(None, metadata={"audit_run_id": log.run_id, "rows_inserted": df.height})
