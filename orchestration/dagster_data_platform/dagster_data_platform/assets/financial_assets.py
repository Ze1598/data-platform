import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetSelection,
    DefaultSensorStatus,
    Output,
    RunRequest,
    SensorEvaluationContext,
    asset,
    define_asset_job,
    sensor,
)

from dagster_data_platform.assets.dbt_assets import dbt_financial_transactions_assets
from dagster_data_platform.resources.iceberg_resource import IcebergCatalogResource
from dagster_data_platform.resources.postgres_metadata_resource import PostgresMetadataResource
from raw_to_clean import reconcile_schema, validate_schema, write_clean_snapshot

FEED_FRIENDLY_NAME = "financial_transactions"
FEED_POOL = f"feed:{FEED_FRIENDLY_NAME}"
LANDING_SUBDIR = "landing/financial_transactions"
RAW_SUBDIR = "raw/financial_transactions"
ARCHIVE_SUBDIR = "archive/financial_transactions"

# .../orchestration/dagster_data_platform/dagster_data_platform/assets/financial_assets.py
# -> repo root is 4 parents up, same convention as dbt_assets.py's
# REPO_ROOT. Only used as the *local* fallback -- inside the launched pod,
# DATA_LAKE_PATH=/data-lake is set explicitly (dagster.yaml's run_launcher
# env_vars), since /app/data-lake doesn't exist there -- the mount lands at
# the container root, not under the app's own code directory.
REPO_ROOT = Path(__file__).resolve().parents[4]


def _data_lake_dir() -> Path:
    return Path(os.environ["DATA_LAKE_PATH"]) if "DATA_LAKE_PATH" in os.environ else REPO_ROOT / "data-lake"


def _landing_dir() -> Path:
    return _data_lake_dir() / LANDING_SUBDIR


def _raw_dir() -> Path:
    return _data_lake_dir() / RAW_SUBDIR


def _archive_dir() -> Path:
    return _data_lake_dir() / ARCHIVE_SUBDIR


@asset(pool=FEED_POOL)
def landing_financial_transactions(
    context: AssetExecutionContext, postgres_metadata: PostgresMetadataResource
) -> Output[pl.DataFrame]:
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        tracking_group=data_feed["batch_group_friendly_name"],
        stage="landing",
        dagster_run_id=context.run_id,
    ) as log:
        # Reads whatever scripts/generate_financial_reports.py has dropped
        # so far, independently of when it ran -- genuine decoupling
        # between generation and extraction, unlike the customers/sales
        # stubs which generate-then-immediately-consume in one asset chain.
        # infer_schema_length=None scans every row of every file before
        # inferring a column's type, rather than just the first N -- a
        # column that's null across the sampled rows but has real values
        # later on would otherwise crash the read, not just mis-infer (see
        # raw_to_clean.reconcile_schema()'s docstring for the same failure
        # mode hit constructing DataFrames from API JSON).
        landing_dir = _landing_dir()
        csv_files = sorted(landing_dir.glob("*.csv")) if landing_dir.exists() else []
        if csv_files:
            # diagonal_relaxed, not vertical_relaxed -- successive batches
            # can genuinely differ in column set over time (the same
            # schema-evolution scenario reconcile_schema() handles
            # downstream), and vertical_relaxed only tolerates differing
            # dtypes for an *identical* column set, erroring outright
            # ("schema lengths differ") the moment one file has an extra or
            # missing column. diagonal_relaxed unions the columns across
            # files and fills a file that lacks one with null, the same
            # tolerance the old list[dict]-based concatenation had for
            # free.
            df = pl.concat(
                [pl.read_csv(f, infer_schema_length=None) for f in csv_files], how="diagonal_relaxed"
            )
        else:
            df = pl.DataFrame()

        # posted_date is ISO 8601 ("2026-07-11T12:34:56Z") in the CSV --
        # lexicographic string comparison already matches chronological
        # order, no need to parse to datetime just to filter.
        last_watermark = data_feed.get("last_watermark_value")
        if not df.is_empty() and last_watermark is not None:
            df = df.filter(pl.col("posted_date") > last_watermark)

        log.set_counts(
            rows_read=df.height,
            watermark_value_start=last_watermark,
            watermark_value_end=df["posted_date"].max() if not df.is_empty() else last_watermark,
        )

    return Output(df, metadata={"audit_run_id": log.run_id, "row_count": df.height})


@asset(pool=FEED_POOL)
def raw_financial_transactions(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    landing_financial_transactions: pl.DataFrame,
) -> Output[pl.DataFrame]:
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        tracking_group=data_feed["batch_group_friendly_name"],
        stage="raw",
        dagster_run_id=context.run_id,
    ) as log:
        df = landing_financial_transactions

        # raw = a verbatim, durable, platform-internal copy of whatever
        # landing had this run -- not a transformation (clean is where
        # parsing/typing happens; raw_to_clean.reconcile_schema() etc. only
        # ever touch the in-memory df, never this copy). Copies the actual
        # landing *files* byte-for-byte, not a re-serialization of the
        # DataFrame -- this run's raw/run_id=<id>/ folder is then the
        # single source of truth for archive_financial_transactions below:
        # whatever filenames land here are exactly what this run consumed
        # from landing, and exactly what gets archived + wiped from
        # landing once the whole run (through the model layer) succeeds.
        # Landing itself is never touched here -- only read.
        if not df.is_empty():
            raw_run_dir = _raw_dir() / f"run_id={context.run_id}"
            raw_run_dir.mkdir(parents=True, exist_ok=True)
            landing_dir = _landing_dir()
            for f in sorted(landing_dir.glob("*.csv")):
                shutil.copy2(f, raw_run_dir / f.name)

        log.set_counts(rows_read=df.height)

    return Output(df, metadata={"audit_run_id": log.run_id, "row_count": df.height})


@asset(pool=FEED_POOL)
def clean_financial_transactions(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    iceberg_catalog: IcebergCatalogResource,
    raw_financial_transactions: pl.DataFrame,
) -> Output[None]:
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    df = raw_financial_transactions
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        tracking_group=data_feed["batch_group_friendly_name"],
        stage="clean",
        dagster_run_id=context.run_id,
    ) as log:
        # Nothing new since the last watermark -- skip the Iceberg write
        # entirely rather than overwrite with a DataFrame that has no rows
        # to infer a schema from. Relies on at least one batch having
        # existed by the time this feed's clean table needs to exist for
        # dbt staging to `ref()` against (true from the first real run
        # onward, since a batch is generated before the pipeline is first
        # triggered for this feed). Still logged as a success with 0 rows.
        if not df.is_empty():
            iceberg_df = df.with_columns(pl.col("posted_date").str.to_datetime(time_zone="UTC"))
            column_definitions = postgres_metadata.get_current_schema(str(data_feed["id"]))
            reconciliation = reconcile_schema(iceberg_df, column_definitions)
            iceberg_df = reconciliation.df
            schema_changed = reconciliation.updated_column_definitions is not None
            if schema_changed:
                postgres_metadata.update_schema_registry(
                    data_feed_id=str(data_feed["id"]),
                    column_definitions=reconciliation.updated_column_definitions,
                    created_by="clean_financial_transactions",
                )
                column_definitions = reconciliation.updated_column_definitions

            validate_schema(iceberg_df, column_definitions)

            catalog = iceberg_catalog.get_catalog()
            write_clean_snapshot(
                catalog,
                namespace="clean",
                table_name="financial_transactions",
                df=iceberg_df,
                column_definitions=column_definitions,
                schema_changed=schema_changed,
            )
        log.set_counts(rows_inserted=df.height)

    # Advance the watermark only after `clean` has actually succeeded --
    # this line is only reached if the `with` block above didn't raise.
    # See PostgresMetadataResource.update_watermark()'s docstring for why
    # this ordering is the correctness property safe re-run depends on.
    if not df.is_empty():
        new_watermark = df["posted_date"].max()
        postgres_metadata.update_watermark(
            data_feed_id=str(data_feed["id"]), watermark_value=new_watermark, run_id=context.run_id
        )

    return Output(None, metadata={"audit_run_id": log.run_id, "rows_inserted": df.height})


@asset(pool=FEED_POOL, deps=[dbt_financial_transactions_assets])
def archive_financial_transactions(context: AssetExecutionContext) -> Output[None]:
    """Archives this run's raw snapshot and wipes the landing files it
    came from -- only reachable once dbt_financial_transactions_assets has
    fully succeeded (a plain `deps=` dependency; Dagster skips a downstream
    asset entirely if any of its dependencies failed, so a clean/dbt
    failure anywhere upstream leaves landing completely untouched for a
    retry -- no separate error handling needed here for that case).

    archive holds the long-term, disaster-recovery copy: one
    watermark-style timestamped folder (YYYY/MM/DD/HH/MM/SS) per run,
    holding everything that run ingested -- reload archived folders in
    run order to rebuild raw/clean/staging from scratch if ever needed.
    landing is wiped only now, at the very end, deliberately last: if
    anything upstream failed, this asset never runs at all, so landing
    still has the original files and a re-run has something to reprocess
    without needing to restore from archive.
    """
    raw_run_dir = _raw_dir() / f"run_id={context.run_id}"
    if not raw_run_dir.exists() or not any(raw_run_dir.iterdir()):
        # Nothing was new this run (raw_financial_transactions only writes
        # this folder when there were rows to process) -- nothing to
        # archive or wipe. Still a success, just a no-op.
        return Output(None, metadata={"archived_files": 0})

    now = datetime.now(timezone.utc)
    archive_run_dir = _archive_dir() / now.strftime("%Y/%m/%d/%H/%M/%S")
    archive_run_dir.mkdir(parents=True, exist_ok=True)

    landing_dir = _landing_dir()
    archived = 0
    for f in sorted(raw_run_dir.iterdir()):
        shutil.copy2(f, archive_run_dir / f.name)
        archived += 1
        landing_file = landing_dir / f.name
        if landing_file.exists():
            landing_file.unlink()

    return Output(None, metadata={"archived_files": archived, "archive_path": str(archive_run_dir)})


# Scoped to just this feed's chain -- a per-feed sensor triggering the
# whole (implicit) __ASSET_JOB would also re-run every other feed on every
# new financial-transactions file, defeating the point of a file-triggered
# sensor. AssetSelection.assets() takes the actual asset *objects*, not
# their names as strings -- dbt_financial_transactions_assets is a
# multi-asset (one dbt model = one AssetKey inside it), so its own `name=`
# isn't a selectable individual key the way a plain @asset's is.
financial_transactions_job = define_asset_job(
    "financial_transactions_job",
    selection=AssetSelection.assets(
        landing_financial_transactions,
        raw_financial_transactions,
        clean_financial_transactions,
        dbt_financial_transactions_assets,
        archive_financial_transactions,
    ),
)


@sensor(
    job=financial_transactions_job,
    minimum_interval_seconds=30,
    # STOPPED by default -- a sensor that's RUNNING from the moment
    # `dagster dev` starts would immediately fire against whatever CSVs
    # already exist in data-lake/, before anyone's decided to test this
    # feed. Turn on from the UI (or `dagster sensor start`) when wanted.
    default_status=DefaultSensorStatus.STOPPED,
)
def financial_transactions_sensor(context: SensorEvaluationContext):
    landing_dir = _landing_dir()
    if not landing_dir.exists():
        return
    csv_files = sorted(landing_dir.glob("*.csv"))
    if not csv_files:
        return
    latest_file = csv_files[-1]
    # Cursor is the latest filename already seen -- filenames sort
    # chronologically (transactions_<YYYYMMDD_HHMMSS>.csv), so a plain
    # string comparison is enough to detect "a newer batch landed".
    if context.cursor is not None and latest_file.name <= context.cursor:
        return
    context.update_cursor(latest_file.name)
    yield RunRequest(run_key=latest_file.name)
