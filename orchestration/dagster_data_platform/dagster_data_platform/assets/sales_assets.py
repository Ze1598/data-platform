from datetime import datetime, timezone

import numpy as np
import polars as pl
from dagster import AssetExecutionContext, Output, asset

from dagster_data_platform.raw_storage import raw_snapshot_path, read_raw_snapshot, write_raw_snapshot
from dagster_data_platform.resources.iceberg_resource import IcebergCatalogResource
from dagster_data_platform.resources.postgres_metadata_resource import PostgresMetadataResource
from connectors import infer_column_definitions
from raw_to_clean import reconcile_schema, validate_schema, write_clean_snapshot

FEED_FRIENDLY_NAME = "sales"
FEED_POOL = f"feed:{FEED_FRIENDLY_NAME}"

# Stub extraction payload for Phase 6 — a synthetic supermarket sales run,
# standing in for a real POS export until this feed gets a real source.
# Regenerated (not fixed) each materialization, same reasoning as the
# customers stub: prove data actually flows and changes every run.
_BRANCHES = [("A", "Yangon"), ("B", "Mandalay"), ("C", "Naypyitaw")]
_PRODUCT_LINES = [
    "Health and beauty",
    "Electronic accessories",
    "Home and lifestyle",
    "Sports and travel",
    "Food and beverages",
    "Fashion accessories",
]
_PAYMENT_METHODS = ["Cash", "Credit card", "Ewallet"]


def _generate_sales_rows(n: int = 20) -> pl.DataFrame:
    # Vectorized (numpy + Polars) generation -- every column built as one
    # array operation, no per-row Python loop, same reasoning as
    # generate_financial_reports.py's bulk generator.
    rng = np.random.default_rng()
    now = datetime.now(timezone.utc)

    branch_idx = rng.integers(0, len(_BRANCHES), size=n)
    branches = np.array([b[0] for b in _BRANCHES])[branch_idx]
    cities = np.array([b[1] for b in _BRANCHES])[branch_idx]

    unit_price = np.round(rng.uniform(10, 100, size=n), 2)
    quantity = rng.integers(1, 11, size=n)
    subtotal = unit_price * quantity
    tax_amount = np.round(subtotal * 0.05, 2)
    cogs = np.round(subtotal * 0.6, 2)

    customer_types = np.array(["Member", "Normal"])[rng.integers(0, 2, size=n)]
    genders = np.array(["Male", "Female"])[rng.integers(0, 2, size=n)]
    product_lines = np.array(_PRODUCT_LINES)[rng.integers(0, len(_PRODUCT_LINES), size=n)]
    payment_methods = np.array(_PAYMENT_METHODS)[rng.integers(0, len(_PAYMENT_METHODS), size=n)]
    ratings = np.round(rng.uniform(4.0, 10.0, size=n), 1)
    minutes_ago = rng.integers(0, 1440, size=n)

    return pl.DataFrame(
        {
            "invoice_id": [f"INV-{now:%Y%m%d}-{i:04d}" for i in range(n)],
            "branch": branches,
            "city": cities,
            "customer_type": customer_types,
            "gender": genders,
            "product_line": product_lines,
            "unit_price": unit_price,
            "quantity": quantity,
            "tax_amount": tax_amount,
            "total": np.round(subtotal + tax_amount, 2),
            "payment_method": payment_methods,
            "cogs": cogs,
            "gross_income": np.round(subtotal - cogs, 2),
            "rating": ratings,
            "minutes_ago": minutes_ago,
        }
    ).with_columns(
        (pl.lit(now) - pl.duration(minutes=pl.col("minutes_ago"))).alias("sale_timestamp")
    ).drop("minutes_ago")


@asset(pool=FEED_POOL, group_name=FEED_FRIENDLY_NAME)
def extraction_sales(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
) -> Output[pl.DataFrame]:
    # No stage-log call of its own -- see extraction_customers' identical
    # comment (extraction_assets.py) for why: `landing` no longer exists as
    # a schema-level stage, and step-selection gating happens once, at the
    # master pipeline's job-launch decision, not per-asset.
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    df = _generate_sales_rows()
    # Schema discovery/registry-write is extraction's job, complete before
    # clean_sales ever runs -- clean_sales only reads schema_registry
    # (get_current_schema()), it never writes to it. Skipped entirely when
    # the feed has discovery disabled (schema deemed stable) --
    # schema_registry keeps whatever it already has.
    if data_feed["schema_discovery_enabled"]:
        postgres_metadata.sync_schema_registry(
            data_feed_id=str(data_feed["id"]),
            discovered_column_definitions=infer_column_definitions(df),
            metadata_source_pk=data_feed["source_pk"],
            discovered_primary_key_columns=None,
            created_by="extraction_sales",
        )
    return Output(df, metadata={"row_count": df.height})


@asset(pool=FEED_POOL, group_name=FEED_FRIENDLY_NAME)
def raw_sales(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    extraction_sales: pl.DataFrame,
) -> Output[None]:
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    master_dagster_run_id = context.run.tags["master_dagster_run_id"]
    df = extraction_sales
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        stage="raw",
        master_dagster_run_id=master_dagster_run_id,
        dagster_run_id=context.run_id,
    ) as log:
        # raw = a verbatim, durable, platform-internal copy of whatever was
        # extracted this run -- zero transformation (same contract as
        # raw_police_crimes/raw_customers). No archive step for this feed --
        # synthetic smoketest data, no retention need.
        write_raw_snapshot(FEED_FRIENDLY_NAME, log.storage_watermark, df)
        log.set_counts(
            rows_read=df.height,
            output_path=str(raw_snapshot_path(FEED_FRIENDLY_NAME, log.storage_watermark)) if not df.is_empty() else None,
        )

    return Output(None, metadata={"audit_run_id": log.run_id, "row_count": df.height})


@asset(pool=FEED_POOL, group_name=FEED_FRIENDLY_NAME, deps=["raw_sales"])
def clean_sales(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    iceberg_catalog: IcebergCatalogResource,
) -> Output[None]:
    # Reads raw_sales' durable parquet file back from disk, rather than
    # accepting its DataFrame as an in-memory asset-dependency value -- see
    # clean_customers' identical comment (extraction_assets.py) for the
    # full reasoning (raw_sales is an order-only `deps=` entry, not a
    # function parameter -- a plain parameter crashes live since Dagster's
    # IO manager treats an upstream Output(None) as nothing-to-load).
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    master_dagster_run_id = context.run.tags["master_dagster_run_id"]
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        stage="clean",
        master_dagster_run_id=master_dagster_run_id,
        dagster_run_id=context.run_id,
    ) as log:
        df = read_raw_snapshot(FEED_FRIENDLY_NAME, log.storage_watermark)
        # Read-only against schema_registry -- extraction_sales already
        # discovered/synced it; this step only reads the now-current
        # contract to reconcile/validate/write.
        if not df.is_empty():
            column_definitions = postgres_metadata.get_current_schema(str(data_feed["id"]))
            df = reconcile_schema(df, column_definitions)
            validate_schema(df, column_definitions)

            catalog = iceberg_catalog.get_catalog()
            write_clean_snapshot(
                catalog,
                namespace="clean",
                table_name="sales",
                df=df,
                column_definitions=column_definitions,
            )
        log.set_counts(rows_inserted=df.height)

    return Output(None, metadata={"audit_run_id": log.run_id, "rows_inserted": df.height})
