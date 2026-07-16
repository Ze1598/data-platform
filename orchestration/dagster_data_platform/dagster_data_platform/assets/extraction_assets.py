from datetime import datetime, timezone

import polars as pl
from dagster import AssetExecutionContext, Output, asset

from dagster_data_platform.raw_storage import raw_snapshot_path, read_raw_snapshot, write_raw_snapshot
from dagster_data_platform.resources.iceberg_resource import IcebergCatalogResource
from dagster_data_platform.resources.postgres_metadata_resource import PostgresMetadataResource
from connectors import infer_column_definitions
from raw_to_clean import reconcile_schema, validate_schema, write_clean_snapshot

FEED_FRIENDLY_NAME = "customers"

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
FEED_POOL = f"feed:{FEED_FRIENDLY_NAME}"

# Stub extraction payload for Phase 5 — Phase 6 replaces this asset chain with
# a real SparkApplication reading actual source data (see Roadmap.md,
# "Spark Operator + real raw->clean"). `email` is stamped with the current
# run's timestamp so every materialization visibly changes something,
# proving data actually flows extraction -> raw -> clean -> dbt staging rather
# than each run being a silent no-op.
_BASE_CUSTOMERS = [
    {"customer_id": 1, "name": "Alice"},
    {"customer_id": 2, "name": "Bob Updated"},
    {"customer_id": 3, "name": "Carol"},
    {"customer_id": 4, "name": "Dave"},
    {"customer_id": 5, "name": "Eve"},
]


@asset(pool=FEED_POOL, group_name=FEED_FRIENDLY_NAME)
def extraction_customers(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
) -> Output[pl.DataFrame]:
    # No stage-log call of its own -- there is no schema-level stage left
    # to attribute a bare fetch to now that `landing` is gone (folded into
    # `raw`, see Roadmap.md "Master pipeline orchestration"); raw_customers
    # logs stage="raw" for the fetch-through-durable-write outcome as a
    # whole. Which of the master pipeline's three steps run at all is
    # decided by the master pipeline itself before this job is even
    # launched (a feed with "extraction" deselected never gets
    # EXTRACTION_JOBS[feed] launched this run), not checked in here.
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    now = datetime.now(timezone.utc)
    df = pl.DataFrame(_BASE_CUSTOMERS).with_columns(
        (pl.col("name").str.split(" ").list.first().str.to_lowercase() + "@example.com").alias("email"),
        pl.lit(now).alias("updated_at"),
    )
    # Schema discovery/registry-write is extraction's job, complete before
    # clean_customers ever runs -- clean_customers only reads
    # schema_registry (get_current_schema()), it never writes to it.
    postgres_metadata.sync_schema_registry(
        data_feed_id=str(data_feed["id"]),
        discovered_column_definitions=infer_column_definitions(df),
        metadata_source_pk=data_feed["source_pk"],
        discovered_primary_key_columns=None,
        created_by="extraction_customers",
    )
    return Output(df, metadata={"row_count": df.height})


@asset(pool=FEED_POOL, group_name=FEED_FRIENDLY_NAME)
def raw_customers(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    extraction_customers: pl.DataFrame,
) -> Output[None]:
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    master_dagster_run_id = context.run.tags["master_dagster_run_id"]
    df = extraction_customers
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        stage="raw",
        master_dagster_run_id=master_dagster_run_id,
        dagster_run_id=context.run_id,
    ) as log:
        # raw = a verbatim, durable, platform-internal copy of whatever was
        # extracted this run -- zero transformation (same contract as
        # raw_police_crimes/raw_sales). No archive step for this feed --
        # synthetic smoketest data, no retention need.
        write_raw_snapshot(FEED_FRIENDLY_NAME, context.run_id, df)
        log.set_counts(
            rows_read=df.height,
            output_path=str(raw_snapshot_path(FEED_FRIENDLY_NAME, context.run_id)) if not df.is_empty() else None,
        )

    return Output(None, metadata={"audit_run_id": log.run_id, "row_count": df.height})


@asset(pool=FEED_POOL, group_name=FEED_FRIENDLY_NAME, deps=["raw_customers"])
def clean_customers(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    iceberg_catalog: IcebergCatalogResource,
) -> Output[None]:
    # Reads raw_customers' durable parquet file back from disk, rather than
    # accepting its DataFrame as an in-memory asset-dependency value --
    # "clean can read from that raw storage to do its clean layer work",
    # applied uniformly even though raw+clean share one job/pod here
    # (Roadmap.md "Master pipeline orchestration"). raw_customers is an
    # order-only `deps=` entry, not a function parameter -- confirmed live
    # that declaring it as a plain `raw_customers: None` parameter instead
    # crashes with "missing 1 required positional argument": Dagster's IO
    # manager treats an upstream Output(None) as "nothing to load" and
    # doesn't pass a value at all, rather than passing None itself.
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    master_dagster_run_id = context.run.tags["master_dagster_run_id"]
    df = read_raw_snapshot(FEED_FRIENDLY_NAME, context.run_id)
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        stage="clean",
        master_dagster_run_id=master_dagster_run_id,
        dagster_run_id=context.run_id,
    ) as log:
        # Same real path as clean_sales — PyIceberg for the atomic
        # overwrite (clean is a snapshot per run, not cumulative, Roadmap.md
        # "Layer Model"), Polars for the DataFrame, schema_registry for
        # validation. Read-only against schema_registry -- extraction_customers
        # already discovered/synced it; this step only reads the now-current
        # contract to reconcile/validate/write.
        if not df.is_empty():
            column_definitions = postgres_metadata.get_current_schema(str(data_feed["id"]))
            df = reconcile_schema(df, column_definitions)
            validate_schema(df, column_definitions)

            catalog = iceberg_catalog.get_catalog()
            write_clean_snapshot(
                catalog,
                namespace="clean",
                table_name="customers",
                df=df,
                column_definitions=column_definitions,
            )
        log.set_counts(rows_inserted=df.height)

    return Output(None, metadata={"audit_run_id": log.run_id, "rows_inserted": df.height})
