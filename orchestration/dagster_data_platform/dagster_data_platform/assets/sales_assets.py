import random
from datetime import datetime, timedelta, timezone

import polars as pl
from dagster import AssetExecutionContext, Output, asset

from dagster_data_platform.resources.iceberg_resource import IcebergCatalogResource
from dagster_data_platform.resources.postgres_metadata_resource import PostgresMetadataResource
from raw_to_clean import validate_schema, write_clean_snapshot

FEED_CODE = "sales"
FEED_POOL = f"feed:{FEED_CODE}"

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


def _generate_sales_rows(n: int = 20) -> list[dict]:
    rows = []
    now = datetime.now(timezone.utc)
    for i in range(n):
        branch, city = random.choice(_BRANCHES)
        unit_price = round(random.uniform(10, 100), 2)
        quantity = random.randint(1, 10)
        subtotal = unit_price * quantity
        tax_amount = round(subtotal * 0.05, 2)
        cogs = round(subtotal * 0.6, 2)
        rows.append(
            {
                "invoice_id": f"INV-{now:%Y%m%d}-{i:04d}",
                "branch": branch,
                "city": city,
                "customer_type": random.choice(["Member", "Normal"]),
                "gender": random.choice(["Male", "Female"]),
                "product_line": random.choice(_PRODUCT_LINES),
                "unit_price": unit_price,
                "quantity": quantity,
                "tax_amount": tax_amount,
                "total": round(subtotal + tax_amount, 2),
                "payment_method": random.choice(_PAYMENT_METHODS),
                "cogs": cogs,
                "gross_income": round(subtotal - cogs, 2),
                "rating": round(random.uniform(4.0, 10.0), 1),
                "sale_timestamp": now - timedelta(minutes=random.randint(0, 1440)),
            }
        )
    return rows


@asset(pool=FEED_POOL)
def landing_sales(
    context: AssetExecutionContext, postgres_metadata: PostgresMetadataResource
) -> Output[list[dict]]:
    data_feed = postgres_metadata.get_data_feed(FEED_CODE)
    with postgres_metadata.log_ingestion_step(
        layer="landing",
        feed_type="data_feed",
        data_feed_id=str(data_feed["id"]),
        dagster_run_id=context.run_id,
    ) as log:
        rows = _generate_sales_rows()
        log.set_counts(rows_read=len(rows))

    return Output(rows, metadata={"audit_run_id": log.run_id, "row_count": len(rows)})


@asset(pool=FEED_POOL)
def raw_sales(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    landing_sales: list[dict],
) -> Output[list[dict]]:
    data_feed = postgres_metadata.get_data_feed(FEED_CODE)
    with postgres_metadata.log_ingestion_step(
        layer="raw",
        feed_type="data_feed",
        data_feed_id=str(data_feed["id"]),
        dagster_run_id=context.run_id,
    ) as log:
        # Stub: passes the landing payload through unchanged, same as
        # raw_customers — real raw file writes are still out of scope
        # (Phase 6 is specifically about raw->clean becoming real).
        rows = landing_sales
        log.set_counts(rows_read=len(rows))

    return Output(rows, metadata={"audit_run_id": log.run_id, "row_count": len(rows)})


@asset(pool=FEED_POOL)
def clean_sales(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    iceberg_catalog: IcebergCatalogResource,
    raw_sales: list[dict],
) -> Output[None]:
    data_feed = postgres_metadata.get_data_feed(FEED_CODE)
    with postgres_metadata.log_ingestion_step(
        layer="clean",
        feed_type="data_feed",
        data_feed_id=str(data_feed["id"]),
        dagster_run_id=context.run_id,
    ) as log:
        column_definitions = postgres_metadata.get_current_schema(str(data_feed["id"]))
        df = pl.DataFrame(raw_sales)
        validate_schema(df, column_definitions)

        catalog = iceberg_catalog.get_catalog()
        write_clean_snapshot(
            catalog,
            namespace="clean",
            table_name="sales",
            df=df,
            column_definitions=column_definitions,
        )
        log.set_counts(rows_inserted=len(raw_sales))

    return Output(None, metadata={"audit_run_id": log.run_id, "rows_inserted": len(raw_sales)})
