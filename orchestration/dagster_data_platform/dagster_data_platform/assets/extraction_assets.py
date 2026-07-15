import os
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from dagster import AssetExecutionContext, Output, asset

from dagster_data_platform.resources.iceberg_resource import IcebergCatalogResource
from dagster_data_platform.resources.postgres_metadata_resource import PostgresMetadataResource
from connectors import infer_column_definitions
from raw_to_clean import reconcile_schema, validate_schema, write_clean_snapshot

FEED_FRIENDLY_NAME = "customers"
RAW_SUBDIR = "raw/customers"

# .../orchestration/dagster_data_platform/dagster_data_platform/assets/extraction_assets.py
# -> repo root is 4 parents up, same convention as financial_assets.py's
# REPO_ROOT.
REPO_ROOT = Path(__file__).resolve().parents[4]


def _data_lake_dir() -> Path:
    return Path(os.environ["DATA_LAKE_PATH"]) if "DATA_LAKE_PATH" in os.environ else REPO_ROOT / "data-lake"


def _raw_dir() -> Path:
    return _data_lake_dir() / RAW_SUBDIR
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


@asset(pool=FEED_POOL, group_name=FEED_FRIENDLY_NAME)
def landing_customers(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    pipeline_init_customers: set,
) -> Output[pl.DataFrame]:
    if "extraction" not in pipeline_init_customers:
        return Output(pl.DataFrame(), metadata={"skipped": True, "reason": "extraction not selected"})
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        tracking_group=data_feed["batch_group_friendly_name"],
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


@asset(pool=FEED_POOL, group_name=FEED_FRIENDLY_NAME)
def raw_customers(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    landing_customers: pl.DataFrame,
) -> Output[pl.DataFrame]:
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        tracking_group=data_feed["batch_group_friendly_name"],
        stage="raw",
        dagster_run_id=context.run_id,
    ) as log:
        # raw = a verbatim, durable, platform-internal copy of whatever was
        # extracted this run -- zero transformation (same contract as
        # raw_police_crimes/raw_sales). No archive step for this feed --
        # synthetic smoketest data, no retention need.
        df = landing_customers
        if not df.is_empty():
            raw_run_dir = _raw_dir() / f"run_id={context.run_id}"
            raw_run_dir.mkdir(parents=True, exist_ok=True)
            df.write_parquet(raw_run_dir / "customers.parquet")
        log.set_counts(rows_read=df.height, output_path=str(_raw_dir() / f"run_id={context.run_id}") if not df.is_empty() else None)

    return Output(df, metadata={"audit_run_id": log.run_id, "row_count": df.height})


@asset(pool=FEED_POOL, group_name=FEED_FRIENDLY_NAME)
def clean_customers(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    iceberg_catalog: IcebergCatalogResource,
    raw_customers: pl.DataFrame,
    pipeline_init_customers: set,
) -> Output[None]:
    if "validation" not in pipeline_init_customers:
        return Output(None, metadata={"skipped": True, "reason": "validation not selected"})
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    df = raw_customers
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        tracking_group=data_feed["batch_group_friendly_name"],
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
        sync_result = postgres_metadata.sync_schema_registry(
            data_feed_id=str(data_feed["id"]),
            discovered_column_definitions=infer_column_definitions(df),
            metadata_source_pk=data_feed["source_pk"],
            discovered_primary_key_columns=None,
            created_by="clean_customers",
        )
        df = reconcile_schema(df, sync_result.column_definitions)
        validate_schema(df, sync_result.column_definitions)

        catalog = iceberg_catalog.get_catalog()
        write_clean_snapshot(
            catalog,
            namespace="clean",
            table_name="customers",
            df=df,
            column_definitions=sync_result.column_definitions,
        )
        log.set_counts(rows_inserted=df.height)

    return Output(None, metadata={"audit_run_id": log.run_id, "rows_inserted": df.height})
