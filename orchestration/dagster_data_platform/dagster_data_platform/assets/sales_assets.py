import os
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl
from dagster import AssetExecutionContext, Output, asset

from dagster_data_platform.resources.iceberg_resource import IcebergCatalogResource
from dagster_data_platform.resources.postgres_metadata_resource import PostgresMetadataResource
from connectors import infer_column_definitions
from raw_to_clean import reconcile_schema, validate_schema, write_clean_snapshot

FEED_FRIENDLY_NAME = "sales"
FEED_POOL = f"feed:{FEED_FRIENDLY_NAME}"
RAW_SUBDIR = "raw/sales"

REPO_ROOT = Path(__file__).resolve().parents[4]


def _data_lake_dir() -> Path:
    return Path(os.environ["DATA_LAKE_PATH"]) if "DATA_LAKE_PATH" in os.environ else REPO_ROOT / "data-lake"


def _raw_dir() -> Path:
    return _data_lake_dir() / RAW_SUBDIR

# Stub landing payload for Phase 6 — a synthetic supermarket sales run,
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
def landing_sales(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    pipeline_init_sales: set,
) -> Output[pl.DataFrame]:
    if "extraction" not in pipeline_init_sales:
        return Output(pl.DataFrame(), metadata={"skipped": True, "reason": "extraction not selected"})
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        tracking_group=data_feed["batch_group_friendly_name"],
        stage="landing",
        dagster_run_id=context.run_id,
    ) as log:
        df = _generate_sales_rows()
        log.set_counts(rows_read=df.height)

    return Output(df, metadata={"audit_run_id": log.run_id, "row_count": df.height})


@asset(pool=FEED_POOL, group_name=FEED_FRIENDLY_NAME)
def raw_sales(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    landing_sales: pl.DataFrame,
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
        # raw_police_crimes/raw_customers). No archive step for this feed --
        # synthetic smoketest data, no retention need.
        df = landing_sales
        if not df.is_empty():
            raw_run_dir = _raw_dir() / f"run_id={context.run_id}"
            raw_run_dir.mkdir(parents=True, exist_ok=True)
            df.write_parquet(raw_run_dir / "sales.parquet")
        log.set_counts(rows_read=df.height, output_path=str(_raw_dir() / f"run_id={context.run_id}") if not df.is_empty() else None)

    return Output(df, metadata={"audit_run_id": log.run_id, "row_count": df.height})


@asset(pool=FEED_POOL, group_name=FEED_FRIENDLY_NAME)
def clean_sales(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    iceberg_catalog: IcebergCatalogResource,
    raw_sales: pl.DataFrame,
    pipeline_init_sales: set,
) -> Output[None]:
    if "validation" not in pipeline_init_sales:
        return Output(None, metadata={"skipped": True, "reason": "validation not selected"})
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    df = raw_sales
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        tracking_group=data_feed["batch_group_friendly_name"],
        stage="clean",
        dagster_run_id=context.run_id,
    ) as log:
        sync_result = postgres_metadata.sync_schema_registry(
            data_feed_id=str(data_feed["id"]),
            discovered_column_definitions=infer_column_definitions(df),
            metadata_source_pk=data_feed["source_pk"],
            discovered_primary_key_columns=None,
            created_by="clean_sales",
        )
        df = reconcile_schema(df, sync_result.column_definitions)
        validate_schema(df, sync_result.column_definitions)

        catalog = iceberg_catalog.get_catalog()
        write_clean_snapshot(
            catalog,
            namespace="clean",
            table_name="sales",
            df=df,
            column_definitions=sync_result.column_definitions,
        )
        log.set_counts(rows_inserted=df.height)

    return Output(None, metadata={"audit_run_id": log.run_id, "rows_inserted": df.height})
