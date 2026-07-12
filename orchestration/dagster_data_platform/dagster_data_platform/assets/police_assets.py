import os
from pathlib import Path
from typing import Any

import polars as pl
import requests
from dagster import AssetExecutionContext, AssetSelection, Output, ScheduleDefinition, asset, define_asset_job

from dagster_data_platform.assets.dbt_assets import dbt_police_crimes_assets
from dagster_data_platform.resources.iceberg_resource import IcebergCatalogResource
from dagster_data_platform.resources.postgres_metadata_resource import PostgresMetadataResource
from raw_to_clean import reconcile_schema, validate_schema, write_clean_snapshot

FEED_FRIENDLY_NAME = "police_crimes"
FEED_POOL = f"feed:{FEED_FRIENDLY_NAME}"
RAW_SUBDIR = "raw/police_crimes"

_API_BASE = "https://data.police.uk/api"
# Central London -- kept fixed to bound volume. Street-level crime data for
# the whole UK is enormous; one force area for one point is a few thousand
# records per month, confirmed against a live call before designing this
# (see the Phase 9 plan / Learnings.md).
_LAT, _LNG = "51.5074", "-0.1278"

# .../orchestration/dagster_data_platform/dagster_data_platform/assets/police_assets.py
# -> repo root is 4 parents up, same convention as financial_assets.py's
# REPO_ROOT. Only used as the *local* fallback -- inside the launched pod,
# DATA_LAKE_PATH=/data-lake is set explicitly (dagster.yaml's run_launcher
# env_vars).
REPO_ROOT = Path(__file__).resolve().parents[4]


def _data_lake_dir() -> Path:
    return Path(os.environ["DATA_LAKE_PATH"]) if "DATA_LAKE_PATH" in os.environ else REPO_ROOT / "data-lake"


def _raw_dir() -> Path:
    return _data_lake_dir() / RAW_SUBDIR


def _months_to_pull(last_watermark: str | None) -> list[str]:
    """Every month strictly after last_watermark through the latest month
    the API currently has data for -- a watermark means "synced up to and
    including here", so a run catches all the way up to what's currently
    available, it doesn't artificially process one month at a time (that
    pattern only makes sense for a dedicated, deliberate historical-backfill
    job, not a regular incremental run; and the watermark should always
    reflect the latest point actually synced to, not an arbitrary partial
    step). Empty on first run only if the API itself has no data at all;
    otherwise the first run's list is every available month, oldest first,
    all pulled in this one run.
    """
    resp = requests.get(f"{_API_BASE}/crimes-street-dates", timeout=30)
    resp.raise_for_status()
    available_months = sorted(entry["date"] for entry in resp.json())
    if last_watermark is None:
        return available_months
    return [m for m in available_months if m > last_watermark]


def _fetch_months(months: list[str]) -> list[dict[str, Any]]:
    # One request per month -- 15 req/s rate limit (burst 30, confirmed
    # against the live API before designing this) makes even several dozen
    # months trivial to pull sequentially in one run, no throttling needed.
    rows = []
    for month in months:
        resp = requests.get(
            f"{_API_BASE}/crimes-street/all-crime",
            params={"lat": _LAT, "lng": _LNG, "date": month},
            timeout=60,
        )
        resp.raise_for_status()
        rows.extend(resp.json())
    return rows


def _outcome_field(df: pl.DataFrame, field: str) -> pl.Expr:
    """outcome_status is null for most crimes (no outcome recorded yet) --
    Polars only infers a Struct dtype for it if at least one row in the
    batch actually has a dict there. If literally every row lacks an
    outcome (plausible early in a month, before any investigations have
    concluded), the whole column infers as Null instead of Struct, and
    .struct.field() has nothing to extract from -- fall back to an
    explicit null column of the right type in that case, rather than
    crashing (the original failure mode this replaces: "could not append
    value ... to the builder", hit constructing a plain list[dict]-based
    DataFrame the same way without this guard)."""
    if isinstance(df.schema["outcome_status"], pl.Struct):
        return pl.col("outcome_status").struct.field(field)
    return pl.lit(None, dtype=pl.Utf8)


@asset(pool=FEED_POOL)
def landing_police_crimes(
    context: AssetExecutionContext, postgres_metadata: PostgresMetadataResource
) -> Output[pl.DataFrame]:
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        tracking_group=data_feed["batch_group_friendly_name"],
        stage="landing",
        dagster_run_id=context.run_id,
    ) as log:
        last_watermark = data_feed.get("last_watermark_value")
        months = _months_to_pull(last_watermark)
        rows = _fetch_months(months) if months else []
        # infer_schema_length=None scans every row before inferring a
        # column's type/struct shape, rather than just the first N -- see
        # _outcome_field()'s docstring for the failure mode this avoids.
        df = pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()

        log.set_counts(
            rows_read=df.height,
            watermark_value_start=last_watermark,
            watermark_value_end=months[-1] if months else last_watermark,
        )

    return Output(df, metadata={"audit_run_id": log.run_id, "row_count": df.height, "months": months})


@asset(pool=FEED_POOL)
def raw_police_crimes(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    landing_police_crimes: pl.DataFrame,
) -> Output[pl.DataFrame]:
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        tracking_group=data_feed["batch_group_friendly_name"],
        stage="raw",
        dagster_run_id=context.run_id,
    ) as log:
        # raw = a verbatim, durable, platform-internal copy of whatever was
        # extracted this run -- zero transformation (flattening the nested
        # location/outcome_status structs is raw_to_clean's job, not raw's;
        # see clean_police_crimes). For a direct-connect source like this
        # API, there's no landing *file* to copy the way
        # raw_financial_transactions copies one -- landing's in-memory
        # extraction result is itself what needs a durable, run-scoped
        # write, so this writes it out rather than copying bytes that
        # already exist on disk. Parquet, not JSON/CSV, since it preserves
        # the nested struct columns (location, outcome_status) without a
        # lossy round-trip.
        df = landing_police_crimes
        if not df.is_empty():
            raw_run_dir = _raw_dir() / f"run_id={context.run_id}"
            raw_run_dir.mkdir(parents=True, exist_ok=True)
            df.write_parquet(raw_run_dir / "crimes.parquet")

        log.set_counts(rows_read=df.height, output_path=str(_raw_dir() / f"run_id={context.run_id}") if not df.is_empty() else None)

    return Output(df, metadata={"audit_run_id": log.run_id, "row_count": df.height})


@asset(pool=FEED_POOL)
def clean_police_crimes(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    iceberg_catalog: IcebergCatalogResource,
    raw_police_crimes: pl.DataFrame,
) -> Output[None]:
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    df = raw_police_crimes
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        tracking_group=data_feed["batch_group_friendly_name"],
        stage="clean",
        dagster_run_id=context.run_id,
    ) as log:
        # Nothing new this run (already caught up to the latest available
        # month) -- skip the Iceberg write, same reasoning as
        # clean_financial_transactions. Still logged as a success with 0
        # rows.
        if not df.is_empty():
            # Genuine parsing, moved here from raw_police_crimes (raw must
            # be a verbatim dump, zero transformation -- see that asset's
            # docstring): flattens the API's nested location/
            # location.street/outcome_status structs into the flat row
            # shape schema_registry expects -- vectorized struct field
            # access, no per-row Python loop.
            df = df.select(
                pl.col("id"),
                pl.col("persistent_id").fill_null(""),
                pl.col("category"),
                pl.col("location_type").fill_null(""),
                pl.col("location_subtype").fill_null(""),
                pl.col("location").struct.field("street").struct.field("id").alias("street_id"),
                pl.col("location").struct.field("street").struct.field("name").alias("street_name"),
                pl.col("location").struct.field("latitude").cast(pl.Float64, strict=False).alias("latitude"),
                pl.col("location").struct.field("longitude").cast(pl.Float64, strict=False).alias("longitude"),
                pl.col("context").fill_null(""),
                pl.col("month"),
                _outcome_field(df, "category").alias("outcome_category"),
                _outcome_field(df, "date").alias("outcome_date"),
            )

            column_definitions = postgres_metadata.get_current_schema(str(data_feed["id"]))
            reconciliation = reconcile_schema(df, column_definitions)
            df = reconciliation.df
            schema_changed = reconciliation.updated_column_definitions is not None
            if schema_changed:
                postgres_metadata.update_schema_registry(
                    data_feed_id=str(data_feed["id"]),
                    column_definitions=reconciliation.updated_column_definitions,
                    created_by="clean_police_crimes",
                )
                column_definitions = reconciliation.updated_column_definitions

            validate_schema(df, column_definitions)

            catalog = iceberg_catalog.get_catalog()
            write_clean_snapshot(
                catalog,
                namespace="clean",
                table_name="police_crimes",
                df=df,
                column_definitions=column_definitions,
                schema_changed=schema_changed,
            )
        log.set_counts(rows_inserted=df.height)

    # Advance the watermark only after `clean` has actually succeeded -- see
    # PostgresMetadataResource.update_watermark()'s docstring.
    if not df.is_empty():
        # A run can now span several months (landing_police_crimes pulls
        # everything since the last watermark through the latest available,
        # not one month at a time) -- advance to the latest month actually
        # processed, not just whichever row happens to be first.
        new_watermark = df["month"].max()
        postgres_metadata.update_watermark(
            data_feed_id=str(data_feed["id"]), watermark_value=new_watermark, run_id=context.run_id
        )

    return Output(None, metadata={"audit_run_id": log.run_id, "rows_inserted": df.height})


# AssetSelection.assets() takes the actual asset *objects*, not their names
# as strings -- dbt_police_crimes_assets is a multi-asset (one dbt model =
# one AssetKey inside it), so its own `name=` isn't a selectable individual
# key the way a plain @asset's is (see financial_assets.py's identical note).
police_crimes_job = define_asset_job(
    "police_crimes_job",
    selection=AssetSelection.assets(
        landing_police_crimes,
        raw_police_crimes,
        clean_police_crimes,
        dbt_police_crimes_assets,
    ),
)

# Cron string hardcoded here rather than read from data_feed.schedule_cron
# at Python import time -- reading Postgres as a module-level side effect
# would make importing this module (e.g. from a future test) silently
# require a live database, the same class of problem Phase 8 hit with
# metadata-driven decisions needing to happen at build/start time, not
# folded into Dagster's own object-graph construction (see Learnings.md,
# "A passing dbt build/test doesn't confirm..." neighbor entry and the
# codegen-is-a-build-time-script lesson). Keep in sync by hand with the
# seeded data_feed.schedule_cron value -- that row is the documented record
# of intent, this constant is what's actually wired.
_SCHEDULE_CRON = "0 6 * * *"

police_crimes_schedule = ScheduleDefinition(job=police_crimes_job, cron_schedule=_SCHEDULE_CRON)
